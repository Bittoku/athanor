# Thin-node P2P — Phase 7 (F7.1): consensus difficulty (DAA) validation for P2P headers

**Status:** design / plan (for review).
**Scope:** F7.1 from `docs/thin-node-p2p-phase7-followups.md` — harden the Phase 6
headers-chain proof-of-work gate from a bare **pow-limit** check to **full consensus
difficulty validation** (the BSV cw-144 DAA), so the P2P cumulative-work tip selector
can never credit an *easier-than-consensus* branch. Source: Hermes review of MR !18,
note 945 blocker 2.

This is the defence-in-depth counterpart promised by F7.2 §1.1: F7.2 made every realtime
producer **advisory** (only the RPC-confirmed reconcile mutates the index), so an
easier-than-consensus branch is already bounded to *wasted reconcile churn* rather than
corruption. F7.1 removes the churn at its source — the P2P tip selector stops crediting
the invalid branch in the first place.

---

## 1. Goal

A header whose compact `bits` is `≤ pow_limit` and whose hash meets its *own* claimed
target — but whose difficulty is **below the consensus-required value for its height** —
must be **rejected** at the headers-tree validation boundary and must never win
cumulative-work tip selection (so it cannot trigger an extend/reorg hint).

The acceptance bar (verbatim from the followup):

> a header with valid `bits ≤ pow_limit` whose hash meets its own claimed target, but
> whose difficulty is below the consensus-required value for its height, is rejected and
> cannot win cumulative-work tip selection.

**Scope of the contract (resolves note-1081 B1 / note-1080 B2).** This acceptance holds
for **every header the node connects above the bootstrap boundary, with no pow-only
exception** — because the bootstrap seeds the full cw-144 ancestor window below the
checkpoint (§D1 / T7.1.8), so the first P2P-learned header already has real ancestors
`P..P-146` and is DAA-validated. The earlier-drafted "accept headers with <147 ancestors
on the pow gate alone" carve-out is **removed**; it directly contradicted this acceptance
(an easier-than-consensus branch forking within 147 blocks of the seed could still be
credited). The only headers F7.1 does **not** DAA-validate are those at or below the
seeded prefix's low edge — ancient/pruned history the thin node never re-validates and
trusts via the checkpoint (the RPC node that produced the checkpoint already enforced
consensus). That region is unreachable by normal P2P extension (§4) and is explicitly
out of scope, not a runtime acceptance path.

### 1.1 What "consensus-required" means on BSV

BSV inherited the Bitcoin-Cash **Nov-2017 DAA** ("cw-144": a rolling 144-block
cumulative-work retarget computed on every block) and has not changed it since. The
consensus difficulty for a block is therefore an **exact** function of its 147 nearest
ancestors' timestamps and targets — not a 2016-block epoch boundary, and not a free
choice bounded only by `pow_limit`. The consensus rule is **exact nBits equality**: the
candidate's raw `BlockHeader.bits(header)` field must equal, bit-for-bit, the canonical
compact the DAA computes from the parent window. F7.1 recomputes that expected `bits` and
rejects any header that does not match. **F7.1 is scoped to the BSV mainnet cw-144 rule**;
the testnet minimum-difficulty exception is stubbed and deferred (§3.3, decision §D3).

---

## 2. Why the current gate is insufficient (the data-shape problem)

Phase 6 validates each header through an injected `:pow_check` seam with signature
`(wire_hash, bits) -> boolean`. **Two different defaults are in play, and the plan must
not conflate them (note-1081 B5):** the *pure* `Tree` defaults `:pow_check` to
`Work.meets_target?/2` (`tree.ex:33-34, 48-61`) — hash-meets-its-own-target only, so the
work/reorg core is unit-testable without mining — while *production* wiring binds the
**pow-limit-aware** `Work.valid_pow?/3` (closing over the resolved `Network.pow_limit`)
in `HeadersChain.build_tree_opts/1` (`headers_chain.ex:308-319`). Either way the signature
is **context-free**: it sees only the header's own hash and `bits`. The DAA, by contrast,
is a function of the **parent window** — the timestamps and targets of ~147 ancestors. A
context-free seam *structurally cannot* express the consensus rule, so F7.1 adds a
*separate* context-aware `daa_check` seam (§4) rather than overloading `pow_check`.

Fortunately the data is already in hand. `HeadersChain.Tree` keys nodes by wire hash to
`%{header, height, work, cum_work, prev}` (`tree.ex:16-20`), so from a candidate's parent
node the validator can walk `prev` to gather every ancestor's `target` (via its `bits`),
`work`, and `cum_work`. The one missing field is the header **timestamp**: `BlockHeader`
today exposes only `hash/1`, `prev_hash[_wire]/1`, and `bits/1` — a `timestamp/1` accessor
(header bytes 68..71 as a little-endian `uint32`) must be **added** (T7.1.0a). No new
*storage* is required — the timestamp already lives in the retained 80-byte `raw` header;
only the accessor and a **wider validation seam** (parent node + bounded ancestor
accessor) are new.

**The window-size tension (resolved by decision §D2, note-1081 B2).** The tree's default
`window: 144` (`tree.ex:41`) retains 144 descendants of the root, and `prune/1` promotes a
new root and **severs older ancestry** (`tree.ex:248-263`). The cw-144 DAA needs the
parent **and** its 146 ancestors (`P` down to `P-146`; see §3.1). 144 < 147, so the
current default cannot satisfy cw-144 even mid-chain, and after pruning "underflow" would
mean *pruned history*, not "near the trusted seed" — which would silently demote normal
post-prune operation to the Phase-6 pow-only gate. F7.1 therefore makes this a **hard
design decision, not an open question**: the retained window is widened to
`147 + reorg_margin` and pruning must preserve the **full DAA ancestor window above the
active validation frontier**. "Underflow" is consequently never a normal-operation state;
if it ever occurs it is a bug and the `daa_check` **fails closed** (rejects the connect),
never falls back to pow-only. See §D2 and T7.1.6.

---

## 3. The consensus core (pure, security-critical)

A new pure module **`Athanor.P2P.HeadersChain.Daa`** computes the expected target. It is
pure (no IO), unit-testable against known mainnet retarget vectors, and the trust floor
of F7.1. All arithmetic is on 256-bit integers (reuse `Work.compact_to_target/1` and a
new `Work.target_to_compact/1`).

### 3.1 `GetSuitableBlock` (median-of-three)

For an anchor block `B`, the "suitable" block is the **median by timestamp** of
`{B, B-1, B-2}` (the standard 3-element timestamp sort). This de-noises the window
endpoints against out-of-order timestamps. Two anchors are used:

- `last  = suitable(P)`        — median-time block of `{P, P-1, P-2}`
- `first = suitable(P-144)`    — median-time block of `{P-144, P-145, P-146}`

where `P` is the candidate header's parent. (Deepest ancestor touched: `P-146`.)

### 3.2 `ComputeTarget` (cw-144)

```
work      = last.cum_work - first.cum_work          # Σ work over (first .. last]
timespan  = last.timestamp - first.timestamp
timespan  = clamp(timespan, 72*600, 288*600)        # [0.5, 2] × (144 × 600s)
projected = work * 600 / timespan                   # 600s = target spacing
next      = (2^256 - projected) / projected         # == floor(2^256 / projected) - 1
next      = min(next, pow_limit_target)             # never easier than pow-limit
expected_bits = target_to_compact(next)
```

Consensus check (corrected per note-1080 B1 / note-1081 B5): the candidate's **raw**
`BlockHeader.bits(header)` field must equal `expected_bits` **exactly**, where
`expected_bits` is the single canonical compact produced by `target_to_compact(next)`.
There is **no candidate-side normalization / round-trip**: the comparison is against the
raw on-wire `bits` u32, so a non-canonical compact that happens to decode to the same
target is **rejected**, matching a full node's `header.nBits == GetNextWorkRequired(...)`
rule. `target_to_compact/1` MUST therefore emit only the canonical encoding (and set the
sign-bit-avoidance high byte exactly as bitcoin-sv does). Round-trip tests stay on the
*encoder/decoder* (`compact_to_target ∘ target_to_compact`), **not** as a candidate
acceptance path; a negative test asserts that a target-equivalent but non-canonical
candidate `bits` is rejected (T7.1.0 / T7.1.5). This mirrors bitcoin-sv's
`GetNextWorkRequired` / `ComputeTarget`. **Speculation/Agent note:** the `(2^256 -
projected) / projected` identity reproduces the reference `(-work)/work` arith_uint256
computation; it must be validated against real mainnet vectors in the test task (T7.1.4),
not trusted from this doc alone.

### 3.3 Testnet minimum-difficulty — scoped out, stubbed, deferred (decision §D3, note-1081 B3)

BSV Testnet3 allows a min-difficulty block: if the candidate's `timestamp >
parent.timestamp + 2 × 600s` (20 minutes) the required target is `pow_limit`, **and**
non-special blocks then retarget from the last non-min-difficulty ancestor — i.e. the full
rule is more than the single 20-minute branch. Mainnet has **no** such rule.

To avoid a knowingly-non-consensus implementation that still claims F7.1 coverage on
testnet, F7.1 is **explicitly scoped to mainnet cw-144 only**. The testnet path is
**network-gated and stubbed**: when `Network.name == :testnet` the `daa_check` returns a
typed `{:error, :testnet_daa_unsupported}` (fail-closed; it does **not** silently accept),
and Athanor's production network resolution remains mainnet (per `Athanor.Blockchain.Network`;
STN is not reachable as a distinct P2P network yet). The full testnet minimum-difficulty
rule + its vectors are **carved out to a dedicated follow-up task** (see §7 mapping). This
removes the earlier "land or stub" ambiguity (former O4): the decision is *stub + defer*,
recorded honestly in the followup mapping so the contract does not overclaim.

---

## 4. Where it plugs in (the validation seam)

`Tree.connect_one/2` (`tree.ex:96-106`) currently rejects a header when
`not tree.pow_check.(hash, bits)`. F7.1 introduces a **context-aware difficulty check**
evaluated *after* the existing pow-limit/meets-target gate (which stays — it is the cheap
fail-fast and the floor the DAA caps against):

1. `pow_check.(hash, bits)` — unchanged Phase 6 gate (hash meets target, target ≤ pow-limit).
2. **new** `daa_check.(parent_node, header, ancestor_fun)` — recompute expected `bits`
   via §3 and require exact raw-`bits` equality.

Both must pass before a node is added and its work credited.

### 4.1 The `daa_check` callback contract (note-1081 B4)

The seam is injected exactly like `pow_check` and has a **fully specified shape** so
implementers and tests target the same layer:

```
daa_check :: (parent_node, header, ancestor_fun) -> :ok | {:error, reason}

  parent_node  :: %{header, height, work, cum_work, prev}   # the in-tree node for P
  header       :: %BlockHeader{}                             # the candidate (child of P)
  ancestor_fun :: (start_node, n :: non_neg_integer) -> node | nil
                  # returns the n-th ancestor of start_node by walking `prev`
                  # (n = 0 → start_node); nil if the walk leaves the retained window

  reason       :: :difficulty_mismatch    # raw bits ≠ canonical expected_bits  → REJECT
                | :insufficient_window     # ancestor_fun returned nil for a needed slot
                | :testnet_daa_unsupported # Network.name == :testnet (§3.3)
```

**Error → decision mapping (all fail closed; none is an accept path):**

| Result | `connect_one` action |
|---|---|
| `:ok` | header connects (after the unchanged `pow_check`) |
| `{:error, :difficulty_mismatch}` | **reject** — not added, no work credited (I1/I5) |
| `{:error, :insufficient_window}` | **reject** — must not occur in normal operation given §D1+§D2; if it does it is a bug, never a pow-only fallback (I3/I6) |
| `{:error, :testnet_daa_unsupported}` | **reject** — F7.1 is mainnet-scoped (§3.3) |

The default production `daa_check` is built in `HeadersChain.build_tree_opts/1` (the same
place `pow_check` is bound), closing over the resolved `Network` (for `pow_limit` and the
`:mainnet`/`:testnet` gate) and delegating to the pure `Daa.expected_bits/3`; tests inject
a bypass to keep the work/reorg core mine-free. `ancestor_fun` is supplied by `Tree`
(it owns `nodes`/`prev`), so `Daa` stays pure (I4): it requests `P..P-146` via the
callback and never reads tree internals directly.

### 4.2 The bootstrap boundary is below the seeded prefix, not at the live tip (note-1081 B1 / note-1080 B2)

Phase 6 plants a single synthetic root (`header: nil`, `tree.ex:52-61`) at the REST/RPC
checkpoint height. That alone leaves the first ~147 P2P-learned headers without a real
ancestor window. F7.1's bootstrap therefore **seeds the full cw-144 ancestor window**:
it fetches the real headers `[seed_height-146 .. seed_height]` (147 real headers) from the
trusted REST/RPC source and plants them as real nodes, with the synthetic `header: nil`
root demoted to `seed_height-147` (decision §D1 / T7.1.8). Consequences:

- The **first DAA-eligible candidate is `seed_height + 1`** — its parent `P = seed_height`
  and `P-146 = seed_height-146` are all real headers, so `GetSuitableBlock` never touches
  the synthetic root. Every header the node connects above `seed_height` is DAA-validated;
  there is **no pow-only acceptance window** above the boundary (this is what makes §1's
  acceptance hold).
- The synthetic root at `seed_height-147` and anything below it is never re-validated and
  is trusted via the checkpoint. It is unreachable by normal forward P2P extension; a peer
  advertising a fork *below* the seeded prefix is deeper than the retained window and is
  refused by the existing locator/window logic (`headers_chain.ex:207`), not silently
  pow-accepted.
- `GetSuitableBlock`/`compute_target` only ever read **real** (`header != nil`) nodes; the
  `:insufficient_window` error exists solely as a fail-closed backstop and is unreachable
  in normal operation given §D1 (seed prefix) + §D2 (window retention).

---

## 5. Invariants

- **I1 — Consensus equality (exact, raw bits).** Every header connected above the
  bootstrap boundary (`seed_height`) has its raw `BlockHeader.bits` equal, bit-for-bit, to
  the canonical cw-144 `expected_bits` for its height. A mismatch — including a
  non-canonical compact that merely decodes to the right target — is rejected: never
  added, no work credited, no `cum_work` contribution.
- **I2 — Floor preserved.** The DAA never *weakens* the Phase-6 gate: a header rejected by
  the pow-limit/meets-target check is still rejected regardless of the DAA result, and the
  DAA-expected target is itself capped at `pow_limit`.
- **I3 — No pow-only acceptance above the boundary.** Because the bootstrap seeds the full
  `P..P-146` real-ancestor window (§D1) and the retained window preserves it (§D2), there
  is **no** height above `seed_height` at which a header is accepted on the Phase-6 pow
  gate alone. The acceptance of §1 holds with no exception; the only un-DAA-validated
  region is the trusted seeded prefix at/below `seed_height-147`, which normal forward P2P
  extension cannot reach.
- **I4 — Purity.** `Daa` is pure (timestamps + targets + cum_work in, expected `bits`
  out); all ancestor lookup is via the injected `ancestor_fun`. No global clock, no
  network, no tree internals.
- **I5 — Tip-selection integrity.** An easier-than-consensus header never enters
  `tree.nodes`, so it can never become `tip` nor contribute to any descendant's
  `cum_work` — closing the followup's acceptance gap structurally.
- **I6 — Fail-closed.** Every `daa_check` error (`:difficulty_mismatch`,
  `:insufficient_window`, `:testnet_daa_unsupported`) rejects the connect. There is no
  code path on which a DAA error degrades to pow-only acceptance.

---

## 6. Task breakdown (TDD; pure-core first)

- **T7.1.0 — `Work.target_to_compact/1`** (canonical encoder) + round-trip property vs
  `compact_to_target/1`, **and** a negative test that a target-equivalent but non-canonical
  compact is *not* emitted (and would not equal a canonical `expected_bits`). The inverse
  encoder the DAA needs to produce `expected_bits`. Pure.
- **T7.1.0a — `BlockHeader.timestamp/1`.** Additive accessor for header bytes 68..71
  (little-endian `uint32`); the DAA window's only currently-missing field. Pure.
- **T7.1.1 — `Daa.suitable/1`** median-of-three selector over an ancestor triple. Pure;
  unit tests incl. out-of-order timestamps and the swap-network edge cases.
- **T7.1.2 — `Daa.compute_target/2`** the cw-144 body (§3.2): work delta, timespan clamp
  (both bounds), projected-work division, `2^256` identity, pow-limit cap. Pure.
- **T7.1.3 — `Daa.expected_bits/3`** the public entry (§4.1 contract): takes the parent
  node + an `ancestor_fun`, assembles `first`/`last` from real ancestors only, returns
  `{:ok, canonical_compact}` or `{:error, :insufficient_window}`. Pure. Unit-test that a
  `nil` from `ancestor_fun` for any needed slot yields `:insufficient_window` (never a
  silent accept).
- **T7.1.4 — mainnet retarget vectors.** Golden test: a handful of real consecutive BSV
  mainnet headers (heights well above DAA activation) — assert `expected_bits` reproduces
  each block's actual `bits`. This is the doc-claim validator for §3.2. *(Vectors to be
  captured from the node / a block explorer and committed as a fixture.)*
- **T7.1.5 — tree integration (the acceptance task).** Add the `daa_check` seam to `Tree`
  with the §4.1 contract, thread the parent node + bounded `ancestor_fun`, enforce after
  the pow gate, map every error to **reject** (§4.1 table / I6). Regressions: (a) an
  easier-than-consensus header at a fully-windowed height is dropped and cannot become tip
  (the followup acceptance bar); (b) a valid header at the same height connects; (c) a
  candidate whose raw `bits` is a target-equivalent **non-canonical** compact is rejected
  (I1); (d) the first header above a seeded boundary (`seed_height + 1`) is DAA-validated,
  i.e. there is **no** pow-only boundary acceptance above `seed_height` (I3).
- **T7.1.6 — window sizing (hard decision §D2).** Set the retained window to
  `147 + reorg_margin` (no "document underflow" alternative) and make `prune/1` preserve the
  full DAA window above the validation frontier. Regression: after pruning, a mid-chain
  candidate still has all of `P..P-146` as real ancestors, and an induced underflow is
  rejected (`:insufficient_window`), never pow-accepted.
- **T7.1.7 — controller wiring.** Default the real `Daa` (network-resolved) through
  `HeadersChain.build_tree_opts/1` alongside `pow_check`. On `:testnet`, `daa_check`
  returns `:testnet_daa_unsupported` (fail-closed stub per §3.3 / §D3) — *not* a silent
  accept. Smoke test on the resolved (mainnet) network; test the testnet stub rejects.
- **T7.1.8 — bootstrap DAA-window seeding (decision §D1).** Extend the Phase 7 bootstrap so
  that, in addition to the checkpoint, it fetches the real headers
  `[seed_height-146 .. seed_height]` from the trusted REST/RPC source and plants them as
  real nodes, demoting the synthetic `header: nil` root to `seed_height-147`. Regression:
  the first P2P-learned header (`seed_height + 1`) has a full real `P..P-146` window and is
  DAA-validated; a fork advertised below the seeded prefix is refused by the window/locator
  logic, not pow-accepted.

Acceptance (followup §F7.1) is regression (a) in T7.1.5, with T7.1.8 guaranteeing it holds
from the first connected header.

---

## 7. Mapping to the source review

| Review item | Resolved by |
|---|---|
| note-945 B2 (easier-than-consensus branch credited) | §3 cw-144 core + §4 seam + I1/I5 |
| followup F7.1 (a) "full DAA validation" (mainnet) | §3 (this doc implements approach **a**) |
| followup F7.1 testnet min-difficulty rule | **deferred** to follow-up "F7.1t — testnet min-difficulty DAA"; F7.1 stubs it fail-closed (§3.3 / §D3) |
| followup F7.1 acceptance test | T7.1.5(a) + T7.1.8 |
| F7.2 §1.1 (F7.1 = defence-in-depth) | §1 (removes reconcile churn at source) |
| MR !21 note-1080 B1 / note-1081 B5 (no candidate normalization) | §3.2 exact raw-`bits` equality + I1 + T7.1.0/T7.1.5(c) |
| MR !21 note-1080 B2 / note-1081 B1 (seed-boundary off-by-one / acceptance conflict) | §4.2 seeded window + §D1 + T7.1.8 + I3 |
| MR !21 note-1081 B2 (window/pruning) | §2 + §D2 + T7.1.6 |
| MR !21 note-1081 B3 (testnet required-vs-stub) | §3.3 + §D3 (mainnet-scoped, stub+defer) |
| MR !21 note-1081 B4 (seam imprecise for TDD) | §4.1 callback contract + error→decision table |

---

## 8. Decisions (resolving the MR !21 review)

The former open questions O3/O4/O5 are now **hard design decisions** (the round-1/round-2
Hermes review required the contract and invariants to agree rather than leaving runtime
behaviour open). O1/O2 remain confirmations of scope.

- **D1 — Bootstrap seeds the full cw-144 ancestor window.** The Phase 7 bootstrap fetches
  the real headers `[seed_height-146 .. seed_height]` from the trusted REST/RPC source and
  plants them as real nodes (synthetic `header: nil` root demoted to `seed_height-147`), so
  every header above `seed_height` is DAA-validated with a real `P..P-146` window. This
  replaces the former O3 "accept <147-ancestor headers on the pow gate" carve-out, which
  contradicted §1's acceptance (note-1081 B1). See §4.2, T7.1.8.
- **D2 — Window retention `147 + reorg_margin`; pruning preserves the DAA window.** The
  tree keeps at least the full cw-144 window above the validation frontier, and `prune/1`
  never severs it. "Underflow" is therefore not a normal-operation state; if it occurs the
  `daa_check` fails closed (`:insufficient_window`), never pow-only fallback. Replaces the
  former O5. See §2, §D2-driven T7.1.6.
- **D3 — Mainnet cw-144 only; testnet min-difficulty stubbed + deferred.** F7.1's runtime
  contract is the mainnet rule. The testnet 20-minute minimum-difficulty rule (and its
  non-special-block fallback) is gated to `:testnet` as a fail-closed stub
  (`:testnet_daa_unsupported`) and carved out to a dedicated follow-up; the §7 mapping
  records the deferral honestly. Replaces the former O4 "land or stub" ambiguity.
- **O1 — Approach (a), confirmed.** The followup offered (a) full DAA or (b) keep P2P
  advisory. F7.2 already shipped (b)'s *authority* model, so F7.1 implements (a) as the
  promised hardening (removes real reconcile churn).
- **O2 — Genesis/pre-DAA regimes out of scope, confirmed.** The thin node seeds from a
  recent REST/RPC checkpoint and never resyncs from genesis, so the legacy 2016-block
  retarget and the BCH EDA era are unreachable; F7.1 validates **cw-144 only** from the
  seeded prefix forward.
