# Thin-node P2P — Phase 7 (F7.2): P2P-driven chain-tip index integration

**Status:** design / plan (for review). Revised after MR !19 review (note 1018).
**Scope:** F7.2 from `docs/thin-node-p2p-phase7-followups.md` — drive the indexer from
the Phase 6 headers chain under a single, explicit tip authority, with a no-gap
recovery invariant and a real bootstrap boundary. (F7.1 — consensus difficulty/retarget
validation — is a separate follow-up; see §1.1 for why F7.2 does **not** depend on it.)

This document is the **holistic design** the Phase 6 split was made to enable: the prior
MR (!18) accumulated eight review rounds of incremental patches in exactly this
subsystem. Here the authority model, the apply path, the producer model, and the
bootstrap are specified together so the implementation has one coherent contract.

---

## 1. Goal

Make the index track the chain tip in near-real-time **without ever trusting an
unconfirmed peer**: the existing RPC node remains the sole authority that mutates the
index, and the Phase 6 P2P headers chain (plus the existing ZMQ / JungleBus listeners)
become **advisory hints** that make the RPC reconcile happen *sooner* than the periodic
poll. This is the **advisory-until-RPC-confirmed** model.

### 1.1 Why advisory (and why F7.2 does not depend on F7.1)

If P2P deltas drove `block_process_contexts` directly, a peer advertising
easier-than-consensus `bits` (which passes the Phase 6 pow-limit gate but not full DAA,
deferred to F7.1) could drive authoritative rollback/apply churn. Making **every**
realtime producer advisory removes that exposure structurally: the only branch ever
applied is the one the **RPC node confirms by hash**, and the node already enforces
consensus difficulty. F7.1 (full DAA in the P2P header validator) therefore becomes
*defense-in-depth* for the hint layer — sharper hints, fewer wasted reconciles — not a
correctness precondition for F7.2. (Resolves note-1018 blocker 1.)

### Invariants (hold for every producer — P2P, RPC, ZMQ, JungleBus)

- **I1 — Contiguity / no gaps.** `block_process_contexts` always forms a contiguous
  height range `[bootstrap .. tip]`, each block's `prev` matching the stored
  predecessor. A block is never recorded above a missing predecessor.
- **I2 — Serialized mutation.** Every rollback + connect is one ordered `BlockProcessor`
  operation; a rollback never races in-flight block work.
- **I3 — Height integrity.** `last_processed_height/0` never exceeds the contiguous
  verified prefix; after a rollback it reflects the fork height even on an empty/failed
  connect.
- **I4 — Single mutation authority.** `block_process_contexts` is mutated **only** by the
  RPC-confirmed reconcile path, serialized through the `TipController`. No producer
  (P2P/ZMQ/JungleBus) mutates the index out-of-band; they only *hint*.
- **I5 — No silent loss.** The index always reconciles toward the current RPC node tip,
  so a missed/dropped hint can never strand the index below the node tip — the next
  reconcile (hint- or poll-triggered) closes the gap.

---

## 2. Core design — one authority, many hints

A single owner, **`Athanor.Indexer.TipController`** (GenServer), owns the index tip. It
**replaces the existing `Athanor.Workers.ChainTipVerifier` RPC poller** (whose poll +
catch-up role it subsumes — see §4): after this phase `ChainTipVerifier` is gone from the
supervision tree, so the periodic RPC poll and every realtime producer share the one
controller. The controller runs RPC **reconcile cycles** that are the only thing that
mutates the index. A cycle is triggered by either the periodic poll **or** a *hint* from
any realtime producer; hints are coalesced (debounced) so a burst triggers one cycle.

```
producers (advisory hints)                 authority (mutation)
  HeadersChain :on_tip  --hint-->  ┐
  ZMQ hashblock         --hint-->  ┤--> TipController --reconcile(by hash vs RPC node)-->
  JungleBus block       --hint-->  ┤        │            apply_branch/2 (one ordered op)
  periodic :tick        ----------> ┘        └--> authority state {bootstrapping|syncing|synced}
```

### States (total transition function)

| State | Meaning | Next |
|-------|---------|------|
| `:bootstrapping` | No contiguous prefix yet; establishing the bootstrap boundary (§5). | → `:syncing` once the boundary block is recorded |
| `:syncing` | Index below the RPC node tip (or a divergence is being recovered). Each cycle reconciles by hash and applies the contiguous canonical prefix. | → `:synced` when a cycle returns `:synced`; stays `:syncing` on `{:catch_up}`/`{:reorg}`/`:defer` |
| `:synced` | Index hash-equals the RPC node tip. | → `:syncing` when a cycle/hint detects the node advanced or diverged |

There is **no P2P-authoritative state** and therefore no "suspend P2P authority"
transition: P2P can never mutate the index, so a deep reorg or unbridgeable P2P fork is
simply a hint that the next RPC reconcile resolves (or `:defer`s on an unproven
ancestor). This subsumes the old `:p2p_live`/`:rpc_recovering` machinery.

A hint carries an optional `candidate_tip_hash` (for logging/metrics only); the cycle
always targets the **RPC node tip**, so durability (note-994 B1) is automatic — there is
no per-event delta to replay or buffer.

---

## 3. The reconcile cycle

`TipController.reconcile_once/1` (run on tick or coalesced hint):

1. `node_height ← RpcClient.get_block_count()` (fail-closed: on error, keep state, retry).
2. Resolve authority gate for **observability only**: which producer is "live"
   (`SourceRouter.resolve(:chain_tip)` primary + peer availability) is logged; it does
   not change that RPC is authoritative. (Honours the router-primary contract from Phase 6
   without granting P2P mutation rights.)
3. Reconcile **by hash** from the common ancestor (§3a) → a plan
   `:synced | {:catch_up, from, to} | {:reorg, ancestor, to} | :defer`.
4. Execute the plan via `apply_branch/2` (§3b) using only the **contiguous** canonical
   prefix; fold the **result** (§3b) into authority state and decide whether to schedule
   an immediate follow-up cycle (more to do) or settle to `:synced`.

### 3a. Reconcile-by-hash (sub-requirements 2, 3) — confirmed-sound, reused

Ported from the review-confirmed MR !18 implementation (`reconcile_plan/4` + the pure
walk), preserved at `5472b2b`:

- Walk down from `min(local, node)` to the highest height where local and node hashes are
  **both known and equal** (the common ancestor); return `:defer` the moment either is
  unknown — never deep-rollback on an unproven ancestor (note 937/941).
- Emit `:synced | {:catch_up, from, to} | {:reorg, ancestor, to}`; build the **contiguous
  canonical prefix** from the start height (stop at the first missing hash; defer if the
  first required hash is missing) — no-gap (note 941/945/979).

### 3b. The single ordered apply op with a result contract (sub-requirements 1, 4; note-1018 blocker 3)

`apply_branch/2` is a **synchronous** `BlockProcessor` operation (a `call`, so it stays
serialized in the mailbox **and** returns a result the controller can act on):

```
apply_branch(server, %{rollback_to: height | nil, connect: [block_hash_binary]}) ::
    {:ok, last_height}             # rolled back (if any) and applied every connect block
  | {:partial, last_height, reason}# rolled back and/or applied a prefix, halted at `reason`
  | {:error, reason}               # could not even roll back / nothing applied
```

Handler (in the `BlockProcessor` process, ordered):
1. If `rollback_to` is an integer: `rollback_to/1`, then set `last_height = rollback_to`
   **before** connecting (I3 — note 941 B3).
2. Reduce `connect` with `Enum.reduce_while/3`, **halting at the first failure** (I1/I2),
   each block through `do_process_block/1` (predecessor guard, §5). Return `{:ok, …}` if
   all connected, `{:partial, last_success_height, reason}` if it halted after ≥1
   rollback/connect, `{:error, reason}` if nothing changed.

The controller uses the result: `:ok` at node tip → `:synced`; `:ok`/`:partial` below
node tip → stay `:syncing` and schedule a follow-up cycle (deterministic replay of the
remainder); `:error`/`:defer` → stay, retry next tick. (Resolves note-1018 blocker 3;
`{:partial}` and failed-first-block are explicit test cases.)

---

## 4. Producer model — hints only; the legacy RPC poller is retired (sub-requirement 8; note-1018 B2; note-1027 B1)

There is exactly **one** index-tip mutation owner — `TipController` — and **every** other
path that currently casts into `BlockProcessor` is either subsumed into the controller or
converted to a hint. After this phase, nothing but `TipController.apply_branch/2` writes
`block_process_contexts` (I2/I4).

**Retired / subsumed (the existing RPC poller):**

- **`Athanor.Workers.ChainTipVerifier`** is the current RPC tip poller and **itself casts
  straight into `BlockProcessor`** (`lib/athanor/workers/chain_tip_verifier.ex:44-46`,
  `:93-95`). Its RPC-poll + catch-up role **is** the `TipController` reconcile cycle (§3),
  so it is **removed from the application supervision tree** and replaced by `TipController`
  — there is no separate RPC poller left casting to `BlockProcessor`. (Resolves
  note-1027 B1: a single mutation owner; the RPC poll cannot bypass `apply_branch/2`.)

**Converted to advisory hints (no direct cast):**

- **`HeadersChain` `:on_tip`** → `TipController.hint(:p2p, candidate_tip_hash)`. Replaces
  the Phase 6 detection-only logger.
- **`ZmqListener` hashblock** (currently `GenServer.cast(BlockProcessor, …)` at
  `lib/athanor/blockchain/zmq_listener.ex:107-110`) → `TipController.hint(:zmq, hash)`.
- **`JungleBusClient` block** (currently casts at
  `lib/athanor/blockchain/jungle_bus_client.ex:174-183`) → `TipController.hint(:junglebus, hash)`.

A hint never mutates the index; it only (debounced) schedules a reconcile cycle, which
applies **only** the RPC-confirmed branch. Acceptance tests prove (a) the supervision tree
has exactly one index-tip mutation owner (no `ChainTipVerifier`), and (b) a
ZMQ/JungleBus/P2P hint — or the RPC poll — during an in-flight handoff/recovery cannot
write `block_process_contexts` except through `apply_branch/2`. (Resolves note-1018 B2 +
note-1027 B1.)

> The existing `BlockProcessor.handle_cast({:process_block_hash, …})` is retained only as
> the controller's internal connect primitive (called from `apply_branch`’s reduce); the
> public out-of-band cast entry points (ZMQ, JungleBus, and the legacy `ChainTipVerifier`
> poll) are removed in T7.S.

---

## 5. Bootstrap boundary (sub-requirement 9; note-994 B2)

Replace the `index_empty?()` missing-predecessor exception with an explicit boundary:

- New config `config :athanor, Athanor.Indexer, bootstrap_height: <non_neg_integer>` (and
  optional `bootstrap_hash`), resolved at startup — the lowest height the thin indexer
  indexes from. Default: the node tip height at first start, captured **once** into a
  persisted `IndexerBootstrap` row so it is stable across restarts.
- `predecessor_status/2` (replacing the `index_empty?` branch of `maybe_handle_reorg/2`):
  a block with a missing predecessor is accepted **only** when it is exactly the bootstrap
  block (`height == bootstrap_height` and, if configured, `hash == bootstrap_hash`). Every
  other missing-predecessor block — for any producer — is `{:error, :missing_predecessor}`
  (acceptance (b)). A mismatching predecessor still rolls back and refuses the child
  (note 979 B2).

The only block without a stored predecessor is the configured bootstrap block, so the
no-gap invariant (I1) is total.

---

## 6. Sub-requirement → design map

| # (from F7.2) | Design element |
|---|---|
| 1 serialized reorg apply | §3b `apply_branch/2` single ordered op (now synchronous w/ result) |
| 2 RPC reconcile by hash | §3a (confirmed-sound, reused) |
| 3 contiguous no-gap connect | §3a contiguous prefix + §3b halt-on-first-fail |
| 4 last_height integrity | §3b step 1 + the `{:partial, last_height, _}` contract |
| 5 deep-reorg handling | §2 — no P2P authority to suspend; RPC reconcile + `:defer` covers it |
| 6 router-primary authority | §3 step 2 — router-primary gate is observability; RPC always mutates |
| 7 solicited detached (Phase 6) | producer side already shipped; here it is just a hint qualifier |
| 8 durable, single authority | §2 always reconcile to node tip (no event replay); §4 hints only |
| 9 bootstrap boundary | §5 configured/persisted boundary; total predecessor guard |
| note-1018 B1 (DAA dependency) | §1.1 advisory-until-RPC-confirmed → F7.2 ⊥ F7.1 |
| note-1018 B2 (ZMQ/JBus) | §4 producers become hints; out-of-band casts removed |
| note-1018 B3 (async apply) | §3b synchronous result contract |
| note-1027 B1 (legacy RPC poller) | §2 + §4 — `ChainTipVerifier` retired/subsumed by `TipController`; single mutation owner (T7.S) |

---

## 7. TDD task breakdown (bottom-up, each independently verifiable)

> Convention (as Phases 1–6): pure reducers first, thin GenServer shells over them,
> real-socket integration last; `@tag :external` live smoke; fail-closed `safe_*`
> wrappers on every `GenServer.call` in the live path; no `Process.sleep`/`Process.alive?`.

- **T7.0 — `TipController.Machine` (pure).** Authority state machine as a pure reducer:
  `step(state, event) -> {state, [action]}`, events
  `:tick | {:hint, source} | {:cycle_result, plan_result}`, actions `:reconcile | :noop`.
  Tests: every §2 transition incl. hint coalescing and the `{:partial}`/`:defer`/`:error`
  result foldings.
- **T7.1 — `BlockProcessor.apply_branch/2`** (synchronous, ordered, result contract) +
  the bootstrap predecessor guard (§5, `predecessor_status/2`). Tests: rollback+connect
  atomic; halt on first failure → `{:partial, last_height, _}`; `last_height` integrity
  for `[]`/`[failing]`/`[ok, failing, …]`; bootstrap block accepted, any other
  missing-predecessor (incl. a high block on an empty index) refused; mismatch rolls
  back + refuses.
- **T7.2 — reconcile-by-hash** (`reconcile_plan/4` + the contiguous-prefix builder),
  ported from `5472b2b`. Tests: the full matrix (synced / behind / same-height divergence
  / orphan-ahead / unknown-hash defer / contiguous-prefix / reorg-prefix).
- **T7.3 — `TipController` GenServer.** Wires Machine + reconcile + `apply_branch` +
  injected RPC/processor seams + debounced hints. Tests: hint coalescing → one cycle;
  a P2P/ZMQ/JungleBus hint triggers a reconcile but only the RPC-confirmed branch is
  applied; a hint during an in-flight cycle cannot bypass `apply_branch` (I4);
  `{:partial}`/failed-first-block leave authority state + replay correct; the deferred-tip
  case (index below node, hint arrives, reconcile catches up without a new announcement —
  acceptance (a)).
- **T7.S — wiring + bootstrap + producer cutover (incl. RPC-poller retirement).** Persisted
  `IndexerBootstrap`; wire `HeadersChain` `:on_tip` → `TipController.hint`; **remove** the
  out-of-band `BlockProcessor` casts from `ZmqListener`/`JungleBusClient`, routing them to
  `TipController.hint`; **remove `Athanor.Workers.ChainTipVerifier` from the application
  supervision tree** (its RPC poll role is the controller's reconcile cycle); start
  `TipController`. Tests: bootstrap row captured once + stable; ZMQ/JungleBus events reach
  the controller as hints and do not write contexts directly; **the app supervision tree
  contains exactly one index-tip mutation owner (`TipController`, no `ChainTipVerifier`)**
  and the RPC poll cannot bypass `apply_branch/2` (note-1027 B1).
- **T7.4 — integration (real sockets, `async: false`).** A fork over loopback drives a
  hint; the controller reconciles against an injected RPC node and applies; assert
  `block_process_contexts` stays contiguous (I1) across extend / higher-work reorg /
  deep-reorg; assert a ZMQ-style hint during recovery cannot bypass the controller.
- **T7.5 — `@tag :external` live smoke.** Against testnet: bootstrap → sync to node tip →
  a P2P hint accelerates a reconcile on a real tip advance, asserting contiguity.

---

## 8. Acceptance

- Every invariant I1–I5 has a regression.
- note-994 (a): index below node tip → hint → reconcile catches up to node tip without a
  new peer announcement (T7.3). note-994 (b): empty index refuses a high block, e.g.
  height 105 with no height-104 context (T7.1).
- note-1018: (B1) no index mutation from unconfirmed P2P — only RPC-confirmed branches are
  applied (T7.3/T7.4); (B2) ZMQ/JungleBus cannot bypass the controller (T7.S/T7.4);
  (B3) `apply_branch` result contract drives authority state, with `{:partial}`/
  failed-first-block tests (T7.1/T7.3).
- note-1027 (B1): the application supervision tree has exactly **one** index-tip mutation
  owner — `TipController`, with `ChainTipVerifier` removed — and neither the RPC poll nor
  any producer can write `block_process_contexts` except through `apply_branch/2` (T7.S/T7.4).
- The reused MR !18 matrix (reconcile, serialized apply, no-gap, last_height) green.
- Full `mix test` clean; warnings-clean (bsv_sdk dep excepted); format fixpoint under the
  1.15.7 import_deps toolchain **and** `mix format --check-formatted`; no
  `Process.sleep`/`Process.alive?` in tests.

---

## 9. Non-goals (this phase)

- F7.1 consensus difficulty/retarget (DAA) validation — separate follow-up after F7.2;
  defense-in-depth for the hint layer (§1.1), not a precondition.
- Historical (genesis-up) indexing — the thin indexer bootstraps near the tip (§5).
