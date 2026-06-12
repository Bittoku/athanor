# Phase 3 — Mempool Observation → Indexer: Contracts + TDD Task Breakdown

Companion to `thin-node-p2p-plan.md` and the Phase 0/1/2 task docs.
Scope: turn the live peer pool (Phase 2) into a **realtime mempool observer** that feeds the existing
indexer. `inv(MSG_TX)` → dedup → rate-limit → `getdata` → on `tx` verify txid → parse → **prefilter →
authoritative `TransactionFilter.matches?/1`** → publish into the existing `TransactionProcessor` tagged
`source: :p2p`. No new validation logic — P2P, ZMQ, and JungleBus must stay convergent.

Depends on Phase 2 (`PeerPool`, `PeerRegistry`, the `{:peer, pid, :frame, %Frame{}}` owner stream) being
merged. **Two integration contracts must be locked first** (the plan calls for this *before* the owning
phase); they are §A and §B below and are implemented as part of Phase 3.

---

## §A — Source-tagging + dedupe contract (plan §2.5)

**Problem.** The current pipeline carries no provider/source, so a tx arriving via P2P and via JungleBus
would be indexed without any record of where it came from, and naive double-publish risks double-work.
Grounded in the code on `main`:
- `TransactionFilter.process_raw_tx/1` → `lib/athanor/indexer/transaction_filter.ex:89` casts
  `{:process_raw_tx, raw_tx_binary}`; the handler (`:133`) scans and casts
  `{:index_tx, tx, matched_addresses, matched_tokens}` (`:145`).
- `TransactionProcessor` `{:index_tx,…}` cast (`:43`) and `{:process_tx,…}` call (`:49`) both invoke
  `do_index_tx(tx, ma, mt, nil)` — the `nil` is the block context (height/hash; `nil` ⇒ mempool).
- `MetaTransaction` (`lib/athanor/schema/meta_transaction.ex:17-26`) has `metadata :map` but **no source field**.

**Decision (locked; matches the plan's recommendation — confirm in review).** Carry `source` as an
optional **4th positional argument**, defaulted so existing callers are unchanged, and record it in
`MetaTransaction.metadata["sources"]` — **no schema migration**.

- `@type source :: :p2p | :zmq | :junglebus | :bitails | :block | :unknown`.
- Thread it:
  - `TransactionFilter.process_raw_tx(raw_tx_binary, source \\ :unknown)` → cast
    `{:process_raw_tx, raw_tx_binary, source}`.
  - `TransactionFilter` handler → cast `{:index_tx, tx, ma, mt, source}`.
  - `TransactionProcessor.process_tx(tx, ma, mt, source \\ :unknown)`; `{:index_tx,…,source}` /
    `{:process_tx,…,source}` → `do_index_tx(tx, ma, mt, block_ctx, source)`.
- **Indexing rule (first-seen-wins).** On insert, set `metadata["sources"] = [to_string(source)]`. On a
  **duplicate** (txid already present), do **not** re-index outputs/UTXOs; only **union** the source into
  `metadata["sources"]` and update per-source lag metrics/logging. Dedupe key = **display-order txid**
  (the one boundary where wire→display conversion happens, per the plan's hash-order rule).
- **Back-compat.** Default `:unknown` preserves today's behavior; the block processor passes `:block`, the
  existing ZMQ/JungleBus realtime paths pass `:zmq`/`:junglebus`. No caller is forced to change in one go.

**Tests (T3.1).** Same txid via `:p2p` and via an existing path → indexed **once**; `metadata["sources"]`
contains **both** (order-independent — either source first); no double-index / double-spend; a brand-new
txid records exactly its one source.

---

## §B — Mempool request lifecycle contract (plan §10.2)

A pure state machine governs the `inv → getdata → tx` exchange so every edge case is explicitly tested.

**State** (`MempoolObserver.Tracker`, pure) — note the deliberate split between *in-flight* and *completed*
so a failed request never suppresses recovery:
- `outstanding` — `%{txid => {peer, requested_at_ms}}`, the **in-flight** requests. While a txid is
  outstanding, concurrent `inv`s for it (from a flood of peers) are ignored — this is the flood dedupe.
- `seen` — txids we have **already successfully ingested** (or decided are irrelevant), with TTL
  deadlines. This dedupes *completed* work so a re-`inv` of an already-processed tx is ignored.
  **`seen` is set only on a successful `tx` outcome — never on `inv` and never on a failure.**
- `budget` — a token-bucket (refilled per `:tick`).

A txid is dedup-suppressed iff it is `outstanding` **or** `seen`. A failure (`notfound`/timeout/peer-down)
clears `outstanding` and leaves the txid **neither** outstanding nor seen, so a different peer's later
`inv` re-requests it (recovery), while a flood of *simultaneous* `inv`s is still collapsed to one request.

**`step(state, event, now_ms) → {state, actions}`** — events and the locked decisions:

| Event | Decision | Action(s) |
|---|---|---|
| `{:inv, txid, peer}` | If `txid` is `outstanding` **or** `seen` → ignore. Else if a rate token is available → mark `outstanding(peer)`, spend a token. Else (flood) → **drop** (it will be re-`inv`d; never block). | `{:getdata, peer, txid}` or none |
| `{:tx, txid, payload, peer}` | Must match an `outstanding` txid → clear it and add `txid` to `seen` (TTL). Unsolicited/duplicate `tx` (not outstanding) → ignore (first-wins). | `{:ingest, payload}` or none |
| `{:notfound, txid, peer}` | Clear `outstanding[txid]`; **do not** add to `seen`, so another peer's `inv` can re-request it. No same-peer retry. | none |
| `:request_timeout` (per txid, ~`N`s) | Clear `outstanding[txid]`; **do not** add to `seen` → re-requestable by any peer's next `inv`. | none |
| `{:peer_down, peer}` | Clear all `outstanding` entries for `peer`; **do not** add them to `seen` → their txids are re-requestable from other peers. | none |
| `:tick` | Refill the token bucket; expire `seen` (TTL) and `outstanding` (past timeout) deadlines. | none |

- **Which peer gets `getdata`:** the one that sent the `inv` (it advertised it has the tx).
- **Backpressure:** in-flight dedup (`outstanding`) + completed dedup (`seen`, TTL ~600 s) + token-bucket
  (~200/s) + per-peer `active: :once` (Phase 1) bound both work and mailbox under `inv` floods; excess is
  dropped, not queued.

**Tests (T3.2).** Each row above as a unit case over the pure reducer (inject `now_ms`): in-flight
dedup-ignore, budget-exhaustion drop, tx-clears-outstanding-and-marks-seen, completed-`seen` re-`inv`
ignored, unsolicited-tx ignored, and the **recovery** cases — **`notfound` → *other* peer `inv` → `getdata`**
and **`peer_down` → *other* peer `inv` → `getdata`** (proving a failure does not suppress re-request),
timeout clears + re-requestable, tick refills/expires.

---

## §C — Outbound command + frame-routing seam (closes the !10 review's blocker 3)

Phase 1/2 established only the **inbound** seam: a `Peer` forwards `{:peer, pid, :frame, %Frame{}}` to its
`owner` (the pool). Phase 3 must (a) **route those frames to the observer** and (b) **command an
already-handshaked `Peer` to write an outbound `getdata`**. Both are defined as production API here — no
test-only sends, no coupling to transport internals.

**1. Outbound command — `Peer.send_frame/3` (new public Peer API).**
`Peer.send_frame(peer, command, payload)` casts to the `Peer` GenServer, which — only when `:ready` —
writes `Frame.encode(network, command, payload)` via its injected `transport` (ignored before `:ready`).
This reuses the exact send path the steady-state loop already uses, so a real `Peer` genuinely emits the
frame. The observer calls `Peer.send_frame(pid, :getdata, getdata_payload)` using the **pid it received in
the inbound `{:peer, pid, :frame, inv}` message** — the same peer that advertised the tx (per §B).

**2. Frame routing — pool `frame_sink`.**
`PeerPool.Config` gains an optional `frame_sink :: pid | nil` (default `nil`, current behavior unchanged).
When set, the pool forwards each post-handshake application frame to it as `{:peer, pid, :frame, frame}`
(it still owns lifecycle `:ready`/`:down`). The `MempoolObserver` registers itself as the pool's
`frame_sink` (wired by `P2P.Supervisor`), so it receives the inbound frame stream **with the originating
pid**, enabling the `send_frame` call above. This keeps the Phase-1 owner-indirection intact (the pool
remains the lifecycle owner; the observer is a frame consumer).

**Tests.** `Peer.send_frame/3` is unit-tested over the `Transport.Fake` (assert the encoded `getdata`
bytes are captured) and exercised end-to-end in T3.4 against a real socket (the `FakePeerServer` receives
the `getdata` and answers with `tx`). The pool `frame_sink` is tested by asserting a forwarded frame
arrives at the configured sink pid.

> Implementation note: `Peer.send_frame/3` and `PeerPool.Config.frame_sink` are small **amendments to the
> Phase-2 modules**, landed as the first Phase-3 task (T3.S) before the observer is built.

---

## Core TDD strategy (same shape as Phase 1/2)
Pure reducers do the deciding; one thin GenServer does IO. `Tracker` (§B) and `Watchlist` selection are
pure; `MempoolObserver` is the shell that owns the `:ets` tables, timers, and the `Peer`/pipeline edges.
RED → GREEN → REFACTOR (+ doc headers) → commit `feat(p2p): <task>` (no AI attribution). No
`Process.sleep`/`Process.alive?`; inject `now_ms` and timer messages. Live test is `@tag :external`.

---

## T3.S — Peer/pool seam amendments (§C) — do FIRST
Land the outbound-command + frame-routing seam before the observer exists.

**RED** — `peer_send_frame_test.exs` + a `peer_pool` frame-sink test:
- `Peer.send_frame(pid, :getdata, payload)` after `:ready` → the injected `Transport.Fake` captures the
  encoded `getdata` frame bytes; called before `:ready` → no write.
- `PeerPool` with `frame_sink: self()` → a forwarded post-handshake frame arrives as
  `{:peer, pid, :frame, %Frame{}}` at the sink; with `frame_sink: nil` (default) → no forwarding
  (unchanged behavior).

**GREEN:** add `Peer.send_frame/3` (cast → steady-state send path, ready-gated); add
`PeerPool.Config.frame_sink` and forward frames to it when set.
**REFACTOR:** reuse the existing `Frame.encode` + transport send; keep the pool's lifecycle ownership.

---

## T3.0 — Watchlist prefilter — `Athanor.P2P.Watchlist`
A cheap `:ets` index to reject obviously-irrelevant txs at P2P ingest rate. **Prefilter only** — it must be
a strict **superset** of `TransactionFilter.matches?/1`, never dropping a real match.

**Superset requirement (the !10 review's blocker 1).** `matches?/1` includes a tx on **either** a watched
**address** (P2PKH hash160) **or** a watched **STAS/STAS3 token** (`transaction_filter.ex:176-231`). An
address-only prefilter would silently drop watched-token txs. The prefilter therefore accepts a tx when
**either**:
  1. any output's **hash160 8-byte prefix** is in the watched-address prefix set, **or**
  2. any output is **STAS/STAS3-template-shaped** (a cheap script-shape check, no full parse) — so *every*
     candidate token tx reaches `matches?/1`, which then enforces the actual watched-token-ID membership.

Over-accepting STAS-shaped outputs of *unwatched* tokens is fine (the prefilter's job is to cheaply reject
the obviously-irrelevant; `matches?/1` is the sole inclusion authority). This keeps the prefilter a true
superset regardless of whether the match is by address or by token.

**RED** — `watchlist_test.exs`:
- `new/0` / `put_address/1` build the `:ets` set of 8-byte hash160 prefixes; `maybe_relevant?/1` returns
  `true` if any output prefix matches **or** any output is STAS-shaped, else `false`.
- a **watched-address** tx → `true`; an unrelated P2PKH tx → `false`; a prefix-collision-but-no-real-match
  tx → still `true` (address over-accept is fine).
- a **watched-token (STAS) tx with no watched-address output** → `true` (blocker-1 case); a non-STAS,
  non-watched tx → `false`.
- **property:** `maybe_relevant?` is a superset of `matches?/1` over generated txs that mix watched
  addresses, watched tokens, and noise — it never returns `false` where `matches?/1` returns a match.

**GREEN:** `:ets` set of address prefixes + a STAS-template shape predicate over outputs (reuse the
`bsv_sdk` template tags, not a re-implementation).
**REFACTOR:** share hash160 extraction and the STAS-shape check with `TransactionFilter`/`bsv_sdk` rather
than duplicating script logic.

---

## T3.1 — Source tagging through the pipeline (§A) — *no schema migration*
**RED** — `transaction_filter_source_test.exs` + `transaction_processor_source_test.exs`:
- `process_raw_tx/2` and `process_tx/4` accept a `source`; default `:unknown` keeps current behavior.
- a matched tx with `source: :p2p` → persisted `metadata["sources"] == ["p2p"]`.
- the **same** txid re-published with `source: :junglebus` → still **one** row; `metadata["sources"]`
  unions to `["p2p", "junglebus"]` (set semantics, order-independent); outputs not re-indexed.

**GREEN:** add the optional arg + cast/call shapes; in `do_index_tx`, branch insert-vs-dedupe on txid and
merge the source set into `metadata`.
**REFACTOR:** one `merge_sources/2`; one display-order txid helper at the boundary.

---

## T3.2 — Mempool request tracker (pure reducer, §B) — `Athanor.P2P.MempoolObserver.Tracker`
**RED** — `mempool_observer/tracker_test.exs`: one test per §B table row, driving `step/3` with injected
`now_ms` and asserting `{state, actions}` (getdata emitted exactly once per fresh inv; dedup; budget
drop; tx clears outstanding + emits `{:ingest, _}`; unsolicited tx ignored; notfound/timeout/peer_down
clear; tick refills + expires).
**GREEN:** pure maps/MapSet + a token-bucket counter; `case {event, state}`.
**REFACTOR:** derive `due?`/`expired?` predicates once.

---

## T3.3 — MempoolObserver GenServer — `Athanor.P2P.MempoolObserver`
The thin shell wiring `Tracker` + `Watchlist` + the `Peer` stream + the pipeline.

**RED** — `mempool_observer/mempool_observer_test.exs` (fake peer + a stub pipeline sink):
- registers as the pool's `frame_sink` (§C); on `{:peer, pid, :frame, %Frame{command: "inv", …}}` with an
  unseen `MSG_TX` txid → calls `Peer.send_frame(pid, :getdata, _)` to **that** peer (assert the encoded
  `getdata` is captured by the peer's `Transport.Fake`) and marks it outstanding.
- on `{:peer, pid, :frame, %Frame{command: "tx", payload}}` for an outstanding txid → **re-hashes the
  payload to verify the txid**, parses, runs `Watchlist.maybe_relevant?` then
  `TransactionFilter.matches?/1`, and on a match calls the pipeline with `source: :p2p`; a non-matching
  (prefilter-passing) tx is dropped.
- a `tx` whose recomputed txid ≠ announced → rejected (no ingest).
- `notfound` / request-timeout / peer-down paths drive `Tracker` and are asserted (no ingest, outstanding
  cleared) — timers injected, no sleep.
- inv-flood: beyond the token budget, excess `inv`s yield no `getdata` (assert call count).

**GREEN:** `handle_info` folds frames through `Tracker.step`, performs `{:getdata,_}`/`{:ingest,_}`
actions; `:ets` for `seen`; `Process.send_after` timers for tick + per-request timeout.
**REFACTOR:** one `apply_actions/2`; reuse the Phase-1 owner-message shape.

---

## T3.4 — Integration: P2P inv → tx → indexed (real socket) — `mempool_observer/integration_test.exs` (`async: false`)
End-to-end over a `FakePeerServer` extended to serve a `getdata` with a real `tx`:
- a watched-address payment announced via `inv`, requested via `getdata`, delivered as `tx`, verified,
  matched, and **persisted** with `metadata["sources"] == ["p2p"]` (sandbox DB).
- a tx that passes the prefix prefilter but **fails** `matches?/1` is correctly dropped.

---

## T3.5 — Cross-source dedupe — `mempool_observer/dedupe_test.exs`
The same watched txid arriving via `:p2p` **and** via the existing path (simulated `process_raw_tx/2`):
indexed **once**, `metadata["sources"]` contains both, ordering-independent, no double-index. (Exercises
§A end-to-end against the store.)

---

## T3.6 — Live smoke (manual / CI-skipped) — `@tag :external`
With the pool live on **testnet** and a known watched test address, observe a real mempool payment to it
land in the store via the P2P path within a bounded window (or, if mempool is quiet, assert the
`inv→getdata→tx` exchange completes for *some* tx and the txid verifies). Run with
`mix test --only external`. Documents the one true external dependency; mainnet opt-in via
`P2P_SMOKE_NETWORK=mainnet`.

---

## Design notes the tests enforce (carry into Phase 4+)
- **One inclusion authority.** The `:ets` prefix index is a *prefilter*; `TransactionFilter.matches?/1`
  is the only thing that decides inclusion, so P2P/ZMQ/JungleBus cannot diverge (resolves plan §9.3).
- **Txid is verified, not trusted.** Every `tx` payload is re-hashed before ingest; the announced/wire
  txid is never trusted for inclusion.
- **Pure tracker, thin observer.** All request-lifecycle decisions live in `Tracker` (pure); the
  GenServer only does `:ets`, timers, and the peer/pipeline edges.
- **Source is metadata, not schema (this phase).** `metadata["sources"]` is a set; first-seen wins for
  indexing, later duplicates only union the set + update metrics. A real `source` column is a later,
  metrics-driven migration if needed.

## Definition of Done (Phase 3)
- T3.S, T3.0–T3.5 green; `mix test test/athanor/p2p` + the pipeline tests clean (T3.6 excluded by default).
- No `Process.sleep`/`Process.alive?` in tests; `now_ms`/timers injected.
- A watched-address **and** a watched-token payment seen via P2P land in the **same store** as the
  JungleBus path; cross-source dedupe works on the display-order txid; a prefilter-passing/`matches?`-failing
  tx is dropped; a tx whose recomputed txid mismatches is rejected.
- The §A, §B, and §C contracts are implemented exactly as specified, with their edge-case tests —
  including the Watchlist **token superset** (blocker 1), the **`notfound`/peer-down → other-peer-`inv` →
  re-`getdata`** recovery (blocker 2), and the **`Peer.send_frame/3` + pool `frame_sink`** outbound seam
  (blocker 3), with a real `Peer` proven to emit `getdata`.
- `TransactionFilter.matches?/1` remains the single inclusion authority (no second matcher).

## Suggested commit sequence
`T3.S seam → T3.0 watchlist → T3.1 source_tagging → T3.2 tracker → T3.3 mempool_observer →
T3.4 integration → T3.5 dedupe → T3.6 live_smoke`.
Highest-risk: **T3.2/T3.3** (request lifecycle correctness under floods/disconnects + recovery) and
**T3.4** (real inv→getdata→tx→store).
