# Thin-node P2P — Phase 7 (F7.2): P2P-driven chain-tip index integration

**Status:** design / plan (for review).
**Scope:** F7.2 from `docs/thin-node-p2p-phase7-followups.md` — drive the indexer from
the Phase 6 headers chain under a single, explicit tip authority, with a no-gap
recovery invariant and a real bootstrap boundary. (F7.1 — consensus difficulty/retarget
validation — is a separate follow-up, planned after F7.2.)

This document is the **holistic design** the Phase 6 split was made to enable: the
prior MR (!18) accumulated eight review rounds of incremental patches in exactly this
subsystem. Here the authority model, the apply path, the buffering, and the bootstrap
are specified together so the implementation has one coherent contract to satisfy.

---

## 1. Goal

Replace the Phase 6 detection-only `:on_tip` logger with a bridge that applies the
headers chain's `{:extend}` / `{:reorg}` / `{:reorg_too_deep}` decisions to the index,
and reconcile the existing RPC poll with it so there is **exactly one tip authority at
a time**. Correctness invariants (must hold for every producer — P2P, RPC, ZMQ,
JungleBus):

- **I1 — Contiguity / no gaps.** `block_process_contexts` always forms a contiguous
  height range `[base .. tip]` with each block's `prev` matching the stored
  predecessor. A block is never recorded above a missing predecessor.
- **I2 — Serialized mutation.** Every rollback + connect is one ordered
  `BlockProcessor` mailbox operation; a rollback never races in-flight block work.
- **I3 — Height integrity.** `last_processed_height/0` never exceeds the contiguous
  verified prefix — after a rollback it reflects the fork height even on an empty or
  failed connect.
- **I4 — Single authority.** At any instant the index is driven by exactly one of
  {RPC, P2P}; the other is passive. Authority transitions are explicit and total.
- **I5 — No silent loss.** A P2P best-tip selection is never dropped: if it can't be
  applied now (index below it), it is applied later without requiring a new peer
  announcement.

---

## 2. Core design — the tip-authority state machine

A single owner, **`Athanor.Indexer.TipController`** (a GenServer; supersedes the ad-hoc
deferral logic that lived in `ChainTipVerifier`), holds explicit authority state. The
existing `ChainTipVerifier` RPC poll and the Phase 6 `HeadersChain` `:on_tip` both feed
the controller; the controller is the only thing that mutates the index tip.

### States

| State | Meaning | Who drives the index |
|-------|---------|----------------------|
| `:bootstrapping` | No contiguous verified prefix yet; establishing the bootstrap boundary (§5). | RPC, from the boundary forward |
| `:rpc_catching_up` | Index is behind the P2P best tip (or P2P inactive). Reconcile by hash, process contiguously toward the target. | RPC |
| `:p2p_live` | Index has reached the P2P best tip. P2P events drive extend/reorg; RPC poll is a passive consistency check. | P2P |
| `:rpc_recovering` | A deep reorg (beyond the P2P window) or an unbridgeable fork suspended P2P authority. Reconcile by hash via RPC until caught up. | RPC |

### Transitions (total function; every (state, event) is defined)

```
:bootstrapping     --boundary established-->            :rpc_catching_up
:rpc_catching_up   --local reaches P2P best tip-->      :p2p_live
:rpc_catching_up   --P2P inactive (no peers/route)-->   :rpc_catching_up (stay; RPC remains authority)
:p2p_live          --{:extend} applied contiguously-->  :p2p_live
:p2p_live          --{:reorg} applied-->                :p2p_live
:p2p_live          --{:reorg_too_deep} | P2P inactive-->:rpc_recovering
:p2p_live          --local falls below P2P best tip-->  :rpc_catching_up   (e.g. P2P jumped ahead)
:rpc_recovering    --local reaches P2P best tip
                     AND P2P active again-->            :p2p_live
:rpc_recovering    --P2P still inactive-->              :rpc_recovering
```

**P2P "active"** is defined exactly once: `SourceRouter.resolve(:chain_tip)` primary is
`:p2p` AND there are live peers (the Phase-6 `chain_tip_p2p_active?` predicate, moved
here). This satisfies sub-requirement **6** (router-primary authority): an operator
override `chain_tip: {:rpc, [:p2p]}` keeps the controller permanently RPC-authoritative.

**Deferring to P2P (`:p2p_live`)** requires BOTH P2P active AND `local_tip == p2p_best_tip`
(by hash). This closes note-994 B1 at the state-machine level: the controller never
enters `:p2p_live` while a gap exists, and it never *leaves* RPC authority on the basis
of height alone.

---

## 3. The P2P→index bridge (replaces the detection-only logger)

`HeadersChain`'s `:on_tip` is wired to `TipController.notify_tip/1`, which **casts** the
event to the controller (so work runs in the controller's process — no `HeadersChain`
self-call deadlock; this was learned in MR !18). The controller does not act on the raw
event hashes alone — it treats every event as a **trigger to recompute the P2P
best-chain delta** against the current local tip (see §4), which makes the bridge
robust to dropped/missed events.

- `{:extend, _}` / `{:reorg, _}`: in `:p2p_live`, recompute the delta and apply it via
  the single ordered apply path (§3a). If the recomputed delta starts above the local
  tip (a gap opened), transition to `:rpc_catching_up`.
- `{:reorg_too_deep, _}`: suspend P2P authority → `:rpc_recovering` (sub-requirement 5).
  Cleared when the controller re-reaches the P2P best tip with P2P active.

### 3a. Single ordered apply path — `BlockProcessor.apply_branch/2`

All index mutation funnels through one mailbox op:

```
apply_branch(%{rollback_to: height | nil, connect: [block_hash_binary]}, opts) :: :ok  (cast)
```

`handle_cast({:apply_branch, plan}, state)`:

1. If `rollback_to` is an integer: `rollback_to/1`, then set `last_height = rollback_to`
   **before** connecting (I3 — note 941 B3).
2. Reduce `connect` with `Enum.reduce_while/3`, **halting at the first failure**
   (I1/I2 — note 941/945). Each connect block goes through `do_process_block/1`, which
   enforces the predecessor guard (§5). `last_height` ends at the last contiguous
   success.

This subsumes the Phase-6-era `apply_reorg/3` + `connect_branch/3` (which were sound and
review-confirmed) into one named operation used by **all** producers.

---

## 4. Durable extend — best-chain delta, not event replay (sub-requirement 8)

The failure mode note 994 B1 identified: a P2P `{:extend}` is emitted once; if dropped
(index below seed), it is lost and the index stalls at the seed. The design removes the
reliance on one-shot events:

- `HeadersChain.best_chain_since(local_tip_hash, max)` — a new query returning the
  ordered list of best-chain block hashes (wire→display) **above** `local_tip_hash`, up
  to `max`, or `:detached` if `local_tip_hash` is not on the current best chain (signals
  a reorg). This is computed from the pure `Tree` (it already tracks the best tip and
  ancestry), so it is always current — there is nothing to "miss."
- The controller, on any tip event **or** on its periodic tick, asks
  `best_chain_since(local_tip)`. While in `:rpc_catching_up`/`:rpc_recovering` it uses
  RPC reconcile (§4a) to advance contiguously; once `local_tip` reaches the P2P best tip
  it enters `:p2p_live` and applies subsequent deltas directly. A deferred extension is
  therefore **replayed automatically** the next time the delta is recomputed — no new
  peer announcement required (acceptance (a)).

### 4a. RPC reconcile by hash (sub-requirements 2, 3) — confirmed-sound, reused

When RPC is the authority, the controller reconciles to the node by **hash** (the
review-confirmed `reconcile_plan/4` + `reconcile/3` from MR !18):

- Walk down from `min(local, node)` to the highest height where local and node hashes
  are **both known and equal** (the common ancestor); return `:defer` the moment either
  is unknown (never deep-rollback on an unproven ancestor — note 937/941).
- Emit `:synced` | `{:catch_up, from, to}` | `{:reorg, ancestor, to}`; execute via
  `apply_branch/2` using only the **contiguous** canonical prefix from the start height,
  deferring if the first required hash is missing (no-gap — note 941/945/979).

---

## 5. Bootstrap boundary (sub-requirement 9)

Replace the `index_empty?()` missing-predecessor exception with an explicit boundary:

- New config `config :athanor, Athanor.Indexer, bootstrap_height: <non_neg_integer>`
  (and an optional `bootstrap_hash`), resolved at startup — the lowest supported height
  the thin indexer indexes from (default: the node tip height at first start, captured
  once into a persisted `IndexerBootstrap` row, so it is stable across restarts).
- `maybe_handle_reorg/2` (renamed `predecessor_status/2`): a block with a missing
  predecessor is accepted **only** when it is exactly the bootstrap block (its height ==
  `bootstrap_height` and, if configured, its hash == `bootstrap_hash`). Every other
  missing-predecessor block — for any producer (P2P, RPC, ZMQ, JungleBus) — is refused
  with `{:error, :missing_predecessor}` (acceptance (b)). A mismatching predecessor
  still rolls back and refuses the child (note 979 B2).

This makes the no-gap invariant total: the only block without a stored predecessor is
the configured bootstrap block.

---

## 6. Sub-requirement → design map

| # (from F7.2) | Design element |
|---|---|
| 1 serialized reorg apply | §3a `apply_branch/2` single mailbox op |
| 2 RPC reconcile by hash | §4a `reconcile_plan/4`+`reconcile/3` (confirmed-sound, reused) |
| 3 contiguous no-gap connect | §3a halt-on-first-fail + §4a contiguous prefix |
| 4 last_height integrity | §3a step 1 (set fork height before connect) |
| 5 deep-reorg → RPC authority | §2 `:rpc_recovering` state + §3 `{:reorg_too_deep}` |
| 6 router-primary authority | §2 P2P-active = resolve-primary `:p2p` + peers |
| 7 solicited detached (consumer) | §3 `{:reorg_too_deep}` suspends authority (producer side already in Phase 6) |
| 8 seed handoff + durable extend | §2 enter `:p2p_live` only at `local == p2p_best_tip`; §4 delta recompute (no event replay) |
| 9 bootstrap boundary | §5 configured `bootstrap_height`/hash; total predecessor guard |

---

## 7. TDD task breakdown (bottom-up, each independently verifiable)

> Convention (as Phases 1–6): pure reducers first, thin GenServer shells over them,
> real-socket integration last; `@tag :external` live smoke; fail-closed `safe_*`
> wrappers on every `GenServer.call` in the live path.

- **T7.0 — `TipController.Machine` (pure).** The authority state machine as a pure
  reducer: `step(state, event) -> {state, [action]}` where events are
  `:tick | {:p2p_tip, kind} | {:rpc_result, …} | {:p2p_active?, bool} | {:local_tip, …}`
  and actions are `{:apply_branch, plan}` | `{:set_authority, s}` | `:noop`. Tests: every
  transition row in §2, including the gap-opens and deep-reorg paths.
- **T7.1 — `BlockProcessor.apply_branch/2`** (single ordered op) + the bootstrap
  predecessor guard (§5). Tests: rollback+connect atomic; halt on first failure;
  `last_height` integrity for `[]` and `[failing]`; bootstrap block accepted, any other
  missing-predecessor refused (incl. a high block on an empty index); mismatch rolls
  back + refuses.
- **T7.2 — `HeadersChain.best_chain_since/3`** (pure `Tree` query). Tests: returns the
  ordered delta above a given hash; `:detached` when the hash is off the best chain;
  bounded by `max`.
- **T7.3 — RPC reconcile-by-hash** (`reconcile_plan/4` + `reconcile/3`), ported from the
  confirmed MR !18 implementation at `5472b2b`. Tests: the full matrix (synced / behind /
  same-height divergence / orphan-ahead / unknown-hash defer / contiguous-prefix /
  reorg-prefix).
- **T7.4 — `TipController` GenServer.** Wires Machine + apply_branch + best_chain_since +
  reconcile + the `:chain_tip` router-primary predicate; `:on_tip` casts in. Tests
  (real `HeadersChain` + injected RPC/processor seams): seed handoff (local below seed →
  RPC catch-up → reach seed → `:p2p_live`); **deferred extend replayed** without a new
  announcement (acceptance (a)); `{:reorg_too_deep}` → `:rpc_recovering` → recover;
  router override `{:rpc, [:p2p]}` keeps RPC authoritative.
- **T7.S — supervisor + bootstrap.** Persisted `IndexerBootstrap`; wire `HeadersChain`
  `:on_tip` → `TipController.notify_tip/1`; start `TipController`. Replace the Phase-6
  detection-only logger. Tests: bootstrap row captured once + stable; on_tip wired.
- **T7.5 — integration (real sockets, `async: false`).** Fork-over-sockets driving the
  index through the controller: extend, higher-work reorg (rollback+reapply), deep-reorg
  → RPC recovery; assert `block_process_contexts` stays contiguous throughout (I1).
- **T7.6 — `@tag :external` live smoke.** Against testnet: seed → catch up → `:p2p_live` →
  track a real tip advance, asserting contiguity.

---

## 8. Acceptance

- Every invariant I1–I5 has a regression.
- The two unaddressed note-994 regressions: (a) local-below-seed extend deferred → RPC
  catches local to seed → the same extension applied without a new peer announcement
  (T7.4); (b) empty index refuses a high block, e.g. height 105 with no height-104
  context (T7.1).
- The full MR !18 index-integration matrix (reconcile, serialized apply, no-gap,
  last_height, deep-reorg, router-primary) green.
- Full `mix test` clean; warnings-clean (bsv_sdk dep excepted); format fixpoint under the
  1.15.7 import_deps toolchain; no `Process.sleep`/`Process.alive?` in tests.

---

## 9. Non-goals (this phase)

- F7.1 consensus difficulty/retarget (DAA) validation — separate follow-up after F7.2.
- Historical (genesis-up) indexing — the thin indexer bootstraps near the tip (§5).
