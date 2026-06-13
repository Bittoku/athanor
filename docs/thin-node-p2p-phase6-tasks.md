# Phase 6 — Headers chain + reorg from P2P (TDD task breakdown)

Phases 0–5 made the indexer self-sufficient on the **transaction** plane: wire codec, peer pool, the
inbound mempool observer, the outbound broadcast + relay-back, and the capability router that makes P2P the
primary raw-tx source with an honest REST/RPC fallback. Phase 6 (the plan's **optional** final phase) adds the
**block-header** plane: a P2P-driven headers chain that tracks the best tip by **cumulative work** (not
height), detects reorgs by walking to the common ancestor, and feeds `workers/chain_tip_verifier.ex` from
that instead of polling RPC — while preserving the same cold-start safety the rest of the stack guarantees.

This doc settles the contracts that must be agreed **before** implementation (the algorithmic/security ones
are the crux — §A cumulative-work + PoW validation, §B exchange, §C reorg-set semantics + deep-reorg degraded
state), then the bottom-up TDD tasks. Same shape as Phases 1–5: pure reducers decide, one thin GenServer does
IO; inject time/timers/transport/seams; no `Process.sleep`/`Process.alive?`; commit `feat(p2p): <task>` (no AI
attribution); format under the project's Elixir **1.15** toolchain (import_deps-aware).

The Phase-0 codecs already exist and are reused: `Messages.Headers` (`getheaders`/`headers`),
`Messages.BlockHeader` (`hash/1` wire-order id, `raw` 80 bytes), and `Codec.Hash.double_sha256/1`.

**Byte order — one contract (blocker, settle first).** The tree is **entirely wire/internal order**: node
keys, the `tip`/`root`, and each node's `prev` are all wire-order hashes (`BlockHeader.hash/1` order). The
parent link is therefore the header's **raw** previous-block field (bytes 4..35 of `raw`, *not* reversed) —
**not** `BlockHeader.prev_hash/1`, which returns **display** order for the store layer. To make this
unambiguous in code, T6.0/T6.S add a wire-order accessor `BlockHeader.prev_hash_wire/1` (the raw 32-byte
`prev_block` slice) and the tree uses **only** that for parent lookup; `prev_hash/1` (display) is reserved
**strictly** for the `ChainTipVerifier`/store boundary (§C). So a header whose raw `prev_block` equals a
stored node's `hash/1` connects — it is never mis-classified as detached because of an order mismatch (T6.0
asserts exactly this).

---

## §0 — What this phase does and does NOT do (scope, settle first)

**Does:** maintain a bounded window of recent block **headers** learned over P2P, choose the best tip by
**cumulative proof-of-work**, validate each header's PoW before trusting its work, detect a reorg as a
work-superior branch, and emit the **orphan set** (blocks to disconnect) + **connect set** (blocks to apply)
to the existing tip/reorg consumer.

**Does NOT:** download or validate full blocks or transactions (that stays with `BlockProcessor` over its
existing RPC/JungleBus path); perform the actual DB rollback (Phase 6 only **detects** and emits the sets —
`BlockProcessor`/`ChainTipVerifier` drive the existing rollback machinery); replace REST cold-start (the
headers chain seeds its tip from REST once and falls back to today's RPC `ChainTipVerifier` when P2P is
unavailable — §C).

This boundary keeps Phase 6 a **detection** layer over the block plane, mirroring how the mempool observer is
an ingest layer over the tx plane.

---

## §A — Headers-chain contract (pure `HeadersChain.Tree`)

A pure reducer over a bounded set of headers — no process, no IO, no DB. It is the single authority for "what
is the best tip and did a reorg happen".

**State (`%Tree{}`):**
- `nodes` — `%{wire_hash => %{header: %BlockHeader{}, height: non_neg_integer, work: pos_integer, cum_work:
  pos_integer, prev: wire_hash}}`. Hashes are **wire/internal order** (`BlockHeader.hash/1`), as everywhere in
  the P2P layer; conversion to display order happens only at the store boundary.
- `tip` — the `wire_hash` of the node with the greatest `cum_work` (ties broken by **first-seen**, never by
  height — the plan's explicit rule).
- `root` — the oldest retained header (the window's low edge); `window` (default **144**, ~1 day of blocks) is
  the max retained depth below `tip`.

**Cumulative work (the crux — blocker).** Per-header work is derived from the header's compact `bits` target,
**not** its height:
- decode the 4-byte `bits` (nBits compact form) → `target` (a 256-bit integer); reject `target == 0` or an
  over-large/negative compact mantissa as malformed.
- `work = floor(2^256 / (target + 1))` (the standard Bitcoin "work" of one header).
- `cum_work(node) = cum_work(prev) + work(node)`.
- The best tip is `argmax cum_work` — **height is never used for tip selection** (a shorter, higher-work chain
  wins). T6.0 asserts this explicitly with a higher-work-but-shorter branch.

**Proof-of-work validation (security blocker).** Before a header's `work` is trusted, its PoW MUST verify:
`BlockHeader.hash/1` interpreted as a little-endian 256-bit integer **≤ target** (from its own `bits`). A
header failing PoW is **rejected** (not added) — otherwise a malicious peer could feed forged high-`bits`
headers to fake cumulative work and trigger a bogus reorg. (Difficulty-retarget/timestamp-median validation is
explicitly **out of scope** for this thin-node detector — documented as a known limitation; PoW-per-header is
the trust floor that makes cumulative-work comparison safe against cheap forgery.)

**`step(tree, event) -> {tree, events}`** — events and locked decisions:

| Event | Decision | Emitted |
|---|---|---|
| `{:connect, headers}` (an ordered list from `headers` msg) | For each header in order: let `parent = BlockHeader.prev_hash_wire(header)` (the raw wire-order `prev_block`). If `parent ∈ nodes` **and** PoW valid → add node keyed by `BlockHeader.hash/1` with `prev = parent`, `height = nodes[parent].height+1`, `cum_work` computed; if `parent ∉ nodes` → **detached**, drop it and emit `{:detached, count}` (see below); a header already present is ignored (idempotent); a PoW-invalid header is rejected. After connecting, recompute `tip = argmax cum_work`. If `tip` changed and the new tip is **not** a descendant of the old tip → a **reorg**: walk both tips back (via each node's wire-order `prev`) to the common ancestor; emit `{:reorg, %{orphan: [old-branch hashes tip→fork, exclusive of fork], connect: [new-branch hashes fork→new-tip]}}`. If the new tip simply extends the old tip → emit `{:extend, [new hashes]}`. | `{:extend, hashes}` \| `{:reorg, %{orphan, connect}}` \| `{:detached, count}` \| none |
| `{:locator, n}` | Produce a block **locator** (the getheaders request input): `n` (default ≤ 32) hashes from `tip` backwards with exponential step-back (1,1,2,4,8,…) plus `root`, wire order, for `Messages.Headers.serialize_get_headers/3`. | `{:locator, [wire_hash]}` |
| `:prune` | Drop **every** node — active-path or fork — whose `height < tip.height − window`, and set `root` to the active-path node at `height = tip.height − window` (or the lowest retained active node if shallower). This bounds the tree to ~`window` active-path nodes plus the bounded set of in-window fork nodes; the active spine is **not** retained forever. | none |

- **Detached headers** (parent not in the window) — **single policy: drop-and-re-request.** A header whose
  raw `prev_block` is not a known node is **dropped** by the reducer (it emits `{:detached, count}` for
  observability and stores nothing), and the GenServer responds by issuing a fresh `getheaders` with an
  up-to-date locator so the peer re-sends the missing run **in order** from a point we have. There is **no**
  detached buffer and **no** `max_detached` state — dropping keeps the reducer small and bounds memory to the
  window alone, and `headers` messages already arrive in ancestor→descendant order from a locator so a detached
  header is the signal "my locator was stale", handled by re-requesting, not by buffering. T6.0 asserts a
  detached header leaves the tree unchanged (no node added) and yields `{:detached, _}`; T6.2 asserts the
  shell re-issues `getheaders` on it.
- **Deep-reorg guard (blocker — bounded window honesty).** Any reorg the **reducer** detects is necessarily
  *within* the window: both tips connect to in-window nodes, so their common ancestor is ≥ `root`, and the
  reducer therefore always emits a well-formed `{:reorg, …}` — it **never** fabricates a partial set and
  **never** itself emits `{:reorg_too_deep}`. A reorg whose fork point is **below** the retained window does
  not reach the reducer as a reorg at all: the new branch's first header has a parent below `root`, so it
  arrives as a **detached** header. Escalating that condition is the **GenServer's** job (§B): when successive
  `getheaders`/`headers` rounds keep returning *only* detached headers (our best locator cannot bridge the gap)
  for `max_detached_rounds` (default **3**) consecutive rounds, the shell concludes the fork is deeper than the
  window and emits `{:reorg_too_deep, …}` → operator **alert** + RPC fallback (§C), per the plan §7 "deep reorg
  > window → degraded state" rule. Keeping the reducer pure and pushing the escalation to the shell makes the
  fail-closed path **reachable** instead of an endless re-request loop.
- **Idempotent (within a step); order-convergent only across re-requests.** Connecting the same header twice is
  a no-op (already present → ignored). Headers in one `headers` message arrive ancestor→descendant from a
  locator, so the reducer connects them in a single pass. A child that arrives **before** its parent (e.g.
  split across messages) is **dropped as detached** and only attaches after a network **re-request** —
  convergence is across rounds, not within one reducer step (drop-only does not buffer to reorder).

**Tests (T6.0).** **Byte order:** a header whose raw `prev_block` equals a stored node's `BlockHeader.hash/1`
**connects** (parent found, node added) and is **not** classified as detached — the explicit guard against the
wire/display mismatch. Then: PoW-valid linear extend → `{:extend, …}`, tip advances, `cum_work` increases; a
higher-`work` **shorter** branch overtakes a longer lower-work branch → tip switches by **work not height**;
a fork that switches tip → `{:reorg, %{orphan, connect}}` with the exact common-ancestor split (orphan =
old-branch hashes above the fork, connect = new-branch hashes above the fork); a header failing PoW is
rejected (not added, no work credited); a **detached** header (unknown raw parent) leaves the tree unchanged
(no node added) and yields `{:detached, _}` (the reducer never emits `{:reorg_too_deep}` — that escalation is
the GenServer's, T6.2); `:prune` on a chain longer than `window` drops **every** node below `tip.height −
window` (active-path included), bounding the node count and advancing `root` to the window's low edge;
`:locator` yields a correct exponential-step-back locator.

---

## §B — `HeadersChain` GenServer + the getheaders/headers exchange

The thin shell: a `frame_sink` member that drives the pure `Tree` and performs the wire exchange.

- **Seed once from REST.** On start (when P2P is enabled), seed the tip from the existing
  `ChainTipVerifier`/RPC source: fetch the current tip height+hash via the injected `:seed` seam (default
  `RpcClient.get_block_count/0` + `get_block_hash/1`) and plant it as the `Tree` `root`/`tip` so the locator
  has a starting point. This is the headers-plane analogue of the DXS "seed from REST once" rule.
- **`inv(MSG_BLOCK)` → `getheaders`.** On `{:peer, pid, :frame, %Frame{command: "inv"}}` carrying a
  `MSG_BLOCK` (type 2) vector, ask the `Tree` for a `{:locator, n}` and send
  `Messages.Headers.serialize_get_headers(version, locator, <<0::256>>)` via `Peer.send_frame(pid,
  :getheaders, …)` (de-duplicated per peer with a short cooldown so an inv flood can't amplify into a
  getheaders flood).
- **`headers` → connect.** On `{:peer, _pid, :frame, %Frame{command: "headers"}}`, parse with
  `Messages.Headers.parse/1` (already bounded at `@max_headers = 2000`; a malformed/oversize body is dropped),
  fold `{:connect, headers}` through the `Tree`, and perform the emitted events:
    - `{:extend, hashes}` / `{:reorg, sets}` → reset the consecutive-detached counter to 0, then call the
      injected `:on_tip` sink (§C — feeds `ChainTipVerifier`), carrying display-order hashes at the boundary.
    - `{:detached, _count}` (the run could not attach — a stale locator or a below-window fork) → **re-issue
      `getheaders`** with the current best locator and **increment** the consecutive-detached counter. When it
      reaches `max_detached_rounds` (default **3**), conclude the fork is deeper than the retained window:
      log an **alert**, signal `:on_tip` to fall back to RPC (`{:reorg_too_deep, …}`), and reset the counter.
      This is where the deep-reorg fail-closed path is actually reached (the reducer never emits it).
- **`:tick`** → periodic `:prune` + an opportunistic `getheaders` to keep the window fresh.
- **Fail-closed (consistent with Phase 5).** Every `PeerRegistry`/peer/seed/`on_tip` call that is a
  `GenServer.call`/external call is wrapped so a transient outage degrades (skip this round / fall back to
  RPC) rather than crashing the headers chain — `safe_*` helpers mirroring `TxRelay`/`TxFetcher`.

Injected collaborators: `:seed` (REST tip), `:on_tip` (`(tip_event -> any)`), `:registry`, `:now_fun`,
`:tick_interval_ms`, `:window`, `:max_detached_rounds` (default 3 — the deep-reorg escalation threshold),
`:selector` for which peer(s) to `getheaders` from.

**Wiring.** `HeadersChain` joins the pool fan-out as a `frame_sink` member alongside the observer, relay, and
fetcher: `frame_sink: [MempoolObserver, TxRelay, TxFetcher, HeadersChain]`, supervised under the existing
`:rest_for_one` tree (Registry → … → HeadersChain → Pool). It acts only on `inv(MSG_BLOCK)`/`headers`,
ignoring everything else (disjoint from the other sinks).

**Tests (T6.2).** (The pure reducer is T6.0/§A; the bits→work helper is T6.1.) GenServer (T6.2, real `Peer`s
over `Transport.Fake`): `inv(MSG_BLOCK)` → a `getheaders` with a correct locator reaches the peer; a `headers`
response advances the tip and calls `:on_tip` with `{:extend, …}`; an in-window fork `headers` calls `:on_tip`
with `{:reorg, …}`; a **detached** `headers` run → the shell **re-issues `getheaders`** (assert the second
getheaders on the wire) and, after `max_detached_rounds` consecutive detached rounds, calls `:on_tip` with the
`{:reorg_too_deep}` RPC-fallback signal + logs an alert; a malformed `headers` body is dropped; the seed seam
plants the initial tip; getheaders is de-duplicated under an inv flood; registry/seed exit is fail-safe.

---

## §C — `ChainTipVerifier` integration + cold-start safety

`ChainTipVerifier` today polls RPC (`get_block_count` + `get_block_hash`) every 2 min and only **logs** on a
possible reorg (`local_height > node_height`), never computing orphan/connect sets. Phase 6 feeds it from the
headers chain:

- The `HeadersChain` `:on_tip` sink is wired to a new `ChainTipVerifier` entry point
  `apply_tip_event/1` that:
  - `{:extend, hashes}` → behaves like today's "behind → catch up" for the new heights (hand the block hashes
    to `BlockProcessor` exactly as `catch_up/2` does);
  - `{:reorg, %{orphan, connect}}` → drive the **existing** rollback machinery: roll back the orphaned heights
    (the `BlockProcessor` "rolling back to height" path) then apply the connect set — the headers chain
    supplies *which* blocks, the existing code does the DB work;
  - `{:reorg_too_deep, _}` / any P2P-unavailable signal → **fall back to the current RPC poll** (the existing
    `verify_chain_tip/1` stays as the cold-start/degraded path).
- **Cold-start safety (headline).** With P2P disabled or zero peers, `ChainTipVerifier` runs **exactly as
  today** (RPC poll). The headers chain is a *primary* tip source layered in front; it never removes the RPC
  fallback. Routed through `SourceRouter` as a new `:chain_tip` capability (`{:p2p, [:rpc]}` default) so the
  P2P-vs-RPC choice is config, consistent with Phase 5.

**Tests (T6.3).** `apply_tip_event({:extend, …})` enqueues the new block hashes to `BlockProcessor`;
`apply_tip_event({:reorg, …})` triggers the rollback+apply path with the right heights; `{:reorg_too_deep,…}`
and P2P-disabled both leave the RPC poll behavior unchanged (cold-start parity); the `:chain_tip` route
defaults to `{:p2p, [:rpc]}`.

---

## Tasks (bottom-up, each independently verifiable)

### T6.S — `HeadersChain` frame_sink + supervisor wiring + wire-order prev helper — do FIRST
**RED:** `BlockHeader.prev_hash_wire/1` returns the raw 32-byte `prev_block` (wire order, no reversal),
distinct from the existing display-order `prev_hash/1`; the pool with `frame_sink: [MempoolObserver, TxRelay,
TxFetcher, HeadersChain]` forwards a frame to all four; the supervisor starts `HeadersChain` (Registry →
Observer → TxRelay → TxFetcher → **HeadersChain** → Pool). **GREEN:** add `prev_hash_wire/1`; add the child +
extend the fan-out list. **REFACTOR:** keep single-sink/`nil` fast paths.

### T6.0 — Pure headers tree — `Athanor.P2P.HeadersChain.Tree` (§A)
**RED:** `tree_test.exs` — every §A row (PoW validation, cumulative-work tip selection by work-not-height,
reorg common-ancestor orphan/connect sets, detached headers, deep-reorg guard, prune, locator). **GREEN:** the
reducer (bits→target→work, cum_work, argmax tip, ancestor walk). **REFACTOR:** share the wire/display
convention + `Hash` helpers.

### T6.1 — bits→target→work helper (pure) — `Athanor.P2P.HeadersChain.Work`
Split out the compact-`bits` decode + work formula as a tiny, separately-tested pure module (`compact_to_target/1`,
`work/1`), since it is the security-critical numeric core. **RED:** known-vector bits→target→work cases
(incl. a malformed/zero target rejected). **GREEN/REFACTOR:** the formula.

### T6.2 — `HeadersChain` GenServer — `Athanor.P2P.HeadersChain` (§B)
**RED:** `headers_chain_test.exs` (real Peers over `Transport.Fake`, injected `:seed`/`:on_tip`): seed →
`inv(MSG_BLOCK)` → correct `getheaders` locator on the wire; `headers` → `:on_tip {:extend}`; fork → `:on_tip
{:reorg}`; over-deep → fallback signal + alert; malformed headers dropped; getheaders de-duplicated; fail-safe
on registry/seed exit. **GREEN/REFACTOR:** thin shell reusing the Phase-3 `apply_actions` shape + the Phase-5
`safe_*` fail-closed wrappers.

### T6.3 — `ChainTipVerifier` integration (§C) — `workers/chain_tip_verifier.ex` + `SourceRouter`
**RED:** `apply_tip_event/1` for `{:extend}`/`{:reorg}`/`{:reorg_too_deep}`; cold-start parity (P2P
disabled/zero peers → unchanged RPC poll); `:chain_tip` route default `{:p2p, [:rpc]}`. **GREEN:** wire
`HeadersChain` `:on_tip` → `apply_tip_event`; route via `SourceRouter`; keep the RPC `verify_chain_tip/1` as
the fallback. **REFACTOR:** one tip authority; the existing rollback machinery is reused, not duplicated.

### T6.4 — Integration: a fork over real sockets — `headers_chain/integration_test.exs` (`async: false`)
End-to-end through the real `P2P.Supervisor` + a `FakePeerServer` extended to answer `getheaders` with a
configured `headers` sequence. Feed a linear chain (tip extends), then feed a **higher-work fork** from a
shared ancestor; assert the headers chain switches tip by work and emits the correct orphan/connect sets to a
stub `:on_tip`. A second case: a fork deeper than the window → `{:reorg_too_deep}` + RPC-fallback signal.

### T6.5 — Live smoke (`@tag :external`, testnet, CI-skipped) — `headers_chain/live_smoke_test.exs`
Against live peers: seed from REST, then on real `inv(MSG_BLOCK)` issue `getheaders` and assert the headers
chain advances its tip to (near) the node's tip within a bounded window; `P2P_SMOKE_NETWORK=mainnet` opt-in.
(A real reorg can't be induced on demand; the smoke proves the seed→inv→getheaders→headers→extend path live.)

---

## Definition of Done (Phase 6)
- T6.S, T6.0–T6.4 green; `mix test test/athanor/p2p` + the chain-tip tests clean (T6.5 excluded by default).
- No `Process.sleep`/`Process.alive?`; `now_fun`/timers/seeds/peers injected.
- **Best tip by cumulative work, never height:** a shorter higher-work branch wins (asserted); ties broken by
  first-seen.
- **PoW validated per header** before its work is trusted (hash ≤ target from `bits`); a forged/invalid header
  is rejected — so cumulative-work comparison is not cheaply gameable. Difficulty-retarget/timestamp validation
  is a documented out-of-scope limitation.
- **Reorg detection emits correct orphan/connect sets** via the common-ancestor walk; a reorg **deeper than the
  retained window** yields `{:reorg_too_deep}` (no fabricated set) → operator alert + RPC fallback (the plan's
  §7 degraded-state rule).
- **Bounded window:** `:prune` keeps the tree to `window` depth off the tip; detached headers are dropped (no
  buffer), so there is no unbounded growth from a junk-streaming peer.
- **`ChainTipVerifier` fed from P2P, RPC-fallback intact:** `{:extend}`/`{:reorg}` drive the **existing**
  catch-up/rollback machinery (Phase 6 detects, does not duplicate DB work); P2P-disabled / zero-peers /
  too-deep all collapse to **exactly today's** RPC poll — cold-start safety, routed via `SourceRouter`
  `:chain_tip` (`{:p2p, [:rpc]}`).
- **Fail-closed (Phase-5 consistency):** every `GenServer.call`/external call in the headers path is wrapped so
  a transient registry/seed/peer outage degrades rather than crashing the headers chain.
- Reuses the Phase-0 `Headers`/`BlockHeader` codecs unchanged. No migration (Phase 6 adds no schema; it drives
  existing rollback). Format clean under Elixir 1.15; app code clean under `mix compile --warnings-as-errors`.

---

## Suggested commit sequence
`feat(p2p): wire HeadersChain frame_sink + supervisor (T6.S)` → `feat(p2p): pure bits→work helper (T6.1)` →
`feat(p2p): pure HeadersChain tree — cumulative-work tip + reorg (T6.0)` →
`feat(p2p): HeadersChain GenServer — getheaders/headers exchange (T6.2)` →
`feat(p2p): feed ChainTipVerifier from P2P headers, RPC fallback (T6.3)` →
`test(p2p): fork-over-sockets headers integration (T6.4)` → `test(p2p): headers live smoke (T6.5)`.
