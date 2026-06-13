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

### 1.1 What "consensus-required" means on BSV

BSV inherited the Bitcoin-Cash **Nov-2017 DAA** ("cw-144": a rolling 144-block
cumulative-work retarget computed on every block) and has not changed it since. The
consensus difficulty for a block is therefore an **exact** function of its 147 nearest
ancestors' timestamps and targets — not a 2016-block epoch boundary, and not a free
choice bounded only by `pow_limit`. The consensus rule is **compact equality**:
`header.bits` must equal the `bits` the DAA computes from the parent window. F7.1
recomputes that expected `bits` and rejects any header that does not match (modulo the
testnet exception in §3.3).

---

## 2. Why the current gate is insufficient (the data-shape problem)

Phase 6 validates each header through an injected `:pow_check` seam with signature
`(wire_hash, bits) -> boolean`, defaulting to `Work.valid_pow?/3`
(`lib/athanor/p2p/headers_chain/work.ex`). That signature is **context-free**: it sees
only the header's own hash and `bits`. The DAA, by contrast, is a function of the
**parent window** — the timestamps and targets of ~147 ancestors. A context-free seam
*structurally cannot* express the consensus rule.

Fortunately the data is already in hand. `HeadersChain.Tree` keys nodes by wire hash to
`%{header, height, work, cum_work, prev}` (`tree.ex:16-20`), so from a candidate's parent
node the validator can walk `prev` to gather every ancestor's `target` (via its `bits`),
`work`, and `cum_work`. The one missing field is the header **timestamp**: `BlockHeader`
today exposes only `hash/1`, `prev_hash[_wire]/1`, and `bits/1` — a `timestamp/1` accessor
(header bytes 68..71 as a little-endian `uint32`) must be **added** (T7.1.0a). No new
*storage* is required — the timestamp already lives in the retained 80-byte `raw` header;
only the accessor and a **wider validation seam** (parent node + bounded ancestor
accessor) are new.

**The window-size tension.** The tree's default `window: 144` (`tree.ex:41`) retains 144
descendants of the root. The cw-144 DAA needs the parent **and** its 146 ancestors
(`P` down to `P-146`; see §3.1). 144 < 147, so under the current default the DAA window
can underflow even mid-chain. F7.1 must either widen the retained window to `≥ 147`
(proposed: a `daa_window` margin so the tree keeps `147 + reorg_depth` nodes) or define
DAA as **not-yet-applicable** until ≥147 ancestors are in-window (see §4 and Open
Question O3).

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

Consensus check: `header.bits == expected_bits` (exact compact equality, after a
compact round-trip to normalise non-canonical encodings). This mirrors bitcoin-sv's
`GetNextWorkRequired` / `ComputeTarget`. **Speculation/Agent note:** the `(2^256 -
projected) / projected` identity reproduces the reference `(-work)/work` arith_uint256
computation; it must be validated against real mainnet vectors in the test task (T7.1.4),
not trusted from this doc alone.

### 3.3 Testnet minimum-difficulty exception (out of scope for mainnet)

BSV Testnet3 allows a min-difficulty block: if the candidate's `timestamp >
parent.timestamp + 2 × 600s` (20 minutes), the required target is `pow_limit` regardless
of the DAA. Mainnet has **no** such rule. `Network.t()` already carries `:name`
(`:mainnet | :testnet`) and `:pow_limit`, so the rule is network-gated. F7.1 implements
the mainnet DAA fully; the testnet exception is specified here and implemented behind the
network flag (Open Question O4 asks whether to land it now or stub it).

---

## 4. Where it plugs in (the validation seam)

`Tree.connect_one/2` (`tree.ex:96-106`) currently rejects a header when
`not tree.pow_check.(hash, bits)`. F7.1 introduces a **context-aware difficulty check**
evaluated *after* the existing pow-limit/meets-target gate (which stays — it is the cheap
fail-fast and the floor the DAA caps against):

1. `pow_check.(hash, bits)` — unchanged Phase 6 gate (hash meets target, target ≤ pow-limit).
2. **new** `daa_check.(parent_node, header, ancestor_fun)` — recompute expected `bits`
   via §3 and require equality.

Both must pass before a node is added and its work credited. The `daa_check` seam is
injected exactly like `pow_check` (default = real `Daa` bound to the resolved `Network`;
tests pass a bypass), preserving Phase 6's mine-free unit-testability.

**Applicability near the root.** The synthetic seed root (`header: nil`, trusted
checkpoint from the REST/RPC tip) and any candidate with **fewer than 147 in-window
ancestors** cannot be DAA-validated (the window underflows into/below the trusted seed).
Such headers are accepted on the Phase-6 pow-limit gate alone — they sit at or just above
a trusted checkpoint, so the exposure (an attacker fabricating an easier branch *starting
within 147 blocks of our trusted seed*) is bounded and transient. This boundary is an
explicit, tested invariant (I3 below), not an accident. See Open Question O3.

---

## 5. Invariants

- **I1 — Consensus equality.** A connected non-boundary header's `bits` equals the cw-144
  expected `bits` computed from its parent window (or, on testnet, satisfies §3.3). A
  mismatch is rejected: never added, no work credited, no `cum_work` contribution.
- **I2 — Floor preserved.** The DAA never *weakens* the Phase-6 gate: a header rejected by
  the pow-limit/meets-target check is still rejected regardless of the DAA result, and the
  DAA-expected target is itself capped at `pow_limit`.
- **I3 — Bounded trust boundary.** DAA validation applies exactly to headers with ≥147
  in-window ancestors. Headers nearer the trusted seed are accepted on the Phase-6 gate
  alone; this set shrinks to empty as the window fills above the checkpoint and is never
  silently widened.
- **I4 — Purity.** `Daa` is pure (timestamps + targets + cum_work in, expected `bits`
  out); all IO/lookup is via the injected `ancestor_fun`. No global clock, no network.
- **I5 — Tip-selection integrity.** An easier-than-consensus header never enters
  `tree.nodes`, so it can never become `tip` nor contribute to any descendant's
  `cum_work` — closing the followup's acceptance gap structurally.

---

## 6. Task breakdown (TDD; pure-core first)

- **T7.1.0 — `Work.target_to_compact/1`** (+ round-trip property vs `compact_to_target/1`).
  The inverse encoder the DAA needs to produce `expected_bits`. Pure.
- **T7.1.0a — `BlockHeader.timestamp/1`.** Additive accessor for header bytes 68..71
  (little-endian `uint32`); the DAA window's only currently-missing field. Pure.
- **T7.1.1 — `Daa.suitable/1`** median-of-three selector over an ancestor triple. Pure;
  unit tests incl. out-of-order timestamps and the swap-network edge cases.
- **T7.1.2 — `Daa.compute_target/2`** the cw-144 body (§3.2): work delta, timespan clamp
  (both bounds), projected-work division, `2^256` identity, pow-limit cap. Pure.
- **T7.1.3 — `Daa.expected_bits/3`** the public entry: takes the parent node + an
  `ancestor_fun`, assembles `first`/`last`, returns `{:ok, compact}` or
  `{:error, :insufficient_window}`. Pure.
- **T7.1.4 — mainnet retarget vectors.** Golden test: a handful of real consecutive BSV
  mainnet headers (heights well above DAA activation) — assert `expected_bits` reproduces
  each block's actual `bits`. This is the doc-claim validator for §3.2. *(Vectors to be
  captured from the node / a block explorer and committed as a fixture.)*
- **T7.1.5 — tree integration.** Widen the validation path: add the `daa_check` seam to
  `Tree`, thread the parent node + bounded `ancestor_fun`, enforce after the pow gate,
  apply the §4 boundary rule. Regression: easier-than-consensus header is dropped and
  cannot become tip; a valid header at the same height connects; boundary headers
  (<147 ancestors) still connect on the pow gate.
- **T7.1.6 — window sizing.** Reconcile the retained `window` with the 147-block DAA need
  (widen default or document the underflow→boundary behaviour); regression that a mid-chain
  candidate has a full window.
- **T7.1.7 — controller wiring.** Default the real `Daa` (network-resolved) through
  `HeadersChain.build_tree_opts/1` alongside `pow_check`; testnet min-difficulty behind the
  `Network` flag (or stub per O4). Smoke test on the resolved network.

Acceptance (followup §F7.1) is the assertion in T7.1.5.

---

## 7. Mapping to the source review

| Review item | Resolved by |
|---|---|
| note-945 B2 (easier-than-consensus branch credited) | §3 cw-144 core + §4 seam + I5 |
| followup F7.1 (a) "full DAA validation" | §3 (this doc implements approach **a**) |
| followup F7.1 testnet min-difficulty rule | §3.3 (network-gated) |
| followup F7.1 acceptance test | T7.1.5 |
| F7.2 §1.1 (F7.1 = defence-in-depth) | §1 (removes reconcile churn at source) |

---

## 8. Open questions for review

- **O1 — Approach (a) vs (b).** The followup offered (a) full DAA or (b) keep P2P advisory.
  F7.2 already shipped (b)'s *authority* model, so F7.1 here implements (a) as the
  promised hardening. Confirm we want the full DAA now and not to close F7.1 as
  "satisfied by F7.2's advisory model." *(Recommendation: implement (a) — it is the
  followup's stated requirement and removes real churn.)*
- **O2 — Genesis/pre-DAA regimes out of scope.** The thin node seeds from a recent
  REST/RPC checkpoint and never resyncs from genesis, so the legacy 2016-block retarget
  and the BCH EDA era are unreachable. F7.1 validates **cw-144 only**, from the checkpoint
  forward. Confirm this scoping (vs. a full multi-regime validator).
- **O3 — Boundary policy near the seed.** Accept headers with <147 in-window ancestors on
  the pow-limit gate alone (§4/I3)? The alternative — refuse them until the window fills —
  would stall extension just above a fresh checkpoint. *(Recommendation: accept at the
  boundary; the seed is already trusted.)*
- **O4 — Testnet min-difficulty: land or stub?** Implement §3.3 now (full testnet
  correctness) or stub it behind the network flag and defer to an STN/testnet integration
  task? *(Recommendation: implement — it is small and the followup names it.)*
- **O5 — Window widening.** Widen the tree's retained `window` to `147 + reorg_margin`, or
  keep 144 and treat underflow as the §4 boundary? *(Recommendation: widen — a 144-node
  window that can't satisfy its own cw-144 rule is a latent foot-gun.)*
