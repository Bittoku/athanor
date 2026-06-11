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

**State** (`MempoolObserver.Tracker`, pure): `seen` (dedup set of txids with TTL deadlines), `outstanding`
(`%{txid => {peer, requested_at_ms}}`), and a token-bucket `budget` (refilled per tick).

**`step(state, event, now_ms) → {state, actions}`** — events and the locked decisions:

| Event | Decision | Action(s) |
|---|---|---|
| `{:inv, txid, peer}` | If `txid` already `seen` (within TTL) **or** already `outstanding` → ignore. Else if rate budget available → mark seen + outstanding(peer) + spend a token. Else (flood) → **drop** (it will be re-`inv`d; never block). | `{:getdata, peer, txid}` or none |
| `{:tx, txid, payload, peer}` | Must match an `outstanding` txid → clear it. Unsolicited/duplicate `tx` (not outstanding) → ignore (first-wins). | `{:ingest, payload}` or none |
| `{:notfound, txid, peer}` | Clear `outstanding[txid]`. **Do not retry** in Phase 3 (the tx will be re-`inv`d by another peer, or seen in a block). | none |
| `:request_timeout` (per txid, ~`N`s) | Clear `outstanding[txid]`; leave it in `seen` so we don't immediately re-request. | none |
| `{:peer_down, peer}` | Clear all `outstanding` entries for `peer` (their txids may be re-`inv`d later). | none |
| `:tick` | Refill the token bucket; expire `seen`/`outstanding` past their deadlines. | none |

- **Which peer gets `getdata`:** the one that sent the `inv` (it advertised it has the tx).
- **Backpressure:** dedup (`seen`, TTL ~600 s) + token-bucket (~200/s) + per-peer `active: :once` (Phase 1)
  bound both work and mailbox under `inv` floods; excess is dropped, not queued.

**Tests (T3.2).** Each row above as a unit case over the pure reducer (inject `now_ms`): dedup-ignore,
budget-exhaustion drop, tx-clears-outstanding, unsolicited-tx ignored, notfound clears, timeout clears +
stays seen, peer-down clears that peer's requests, tick refills/expires.

---

## Core TDD strategy (same shape as Phase 1/2)
Pure reducers do the deciding; one thin GenServer does IO. `Tracker` (§B) and `Watchlist` selection are
pure; `MempoolObserver` is the shell that owns the `:ets` tables, timers, and the `Peer`/pipeline edges.
RED → GREEN → REFACTOR (+ doc headers) → commit `feat(p2p): <task>` (no AI attribution). No
`Process.sleep`/`Process.alive?`; inject `now_ms` and timer messages. Live test is `@tag :external`.

---

## T3.0 — Watchlist prefilter — `Athanor.P2P.Watchlist`
A cheap `:ets` hash160 **8-byte-prefix** index to reject obviously-irrelevant txs at P2P ingest rate.
**Prefilter only** — it must never *widen* inclusion.

**RED** — `watchlist_test.exs`:
- `new/0`/`put_address/1` build an `:ets` set of 8-byte hash160 prefixes derived from watched addresses.
- `maybe_relevant?(tx)` returns `true` if **any** output's hash160 prefix is in the set, else `false`.
- a watched address's tx → `true`; an unrelated tx → `false`; a tx whose prefix collides but whose full
  script does **not** match → still `true` (prefilter over-accepts; that's fine — `matches?/1` decides).
- property: `maybe_relevant?` is a *superset* of `matches?/1` (never drops a real match).

**GREEN:** `:ets` set of prefixes; fold over `tx.outputs` extracting hash160, compare 8-byte prefix.
**REFACTOR:** share hash160 extraction with `TransactionFilter` where possible.

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
- subscribes to the pool/peer owner stream; on `{:peer, pid, :frame, %Frame{command: "inv", …}}` with an
  unseen `MSG_TX` txid → sends `getdata` to **that** peer (assert via a fake transport/sink) and marks it
  outstanding.
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
- T3.0–T3.5 green; `mix test test/athanor/p2p` + the pipeline tests clean (T3.6 excluded by default).
- No `Process.sleep`/`Process.alive?` in tests; `now_ms`/timers injected.
- A watched-address payment seen via P2P lands in the **same store** as the JungleBus path; cross-source
  dedupe works on the display-order txid; a prefilter-passing/`matches?`-failing tx is dropped; a tx whose
  recomputed txid mismatches is rejected.
- The §A and §B contracts are implemented exactly as specified above, with their edge-case tests.
- `TransactionFilter.matches?/1` remains the single inclusion authority (no second matcher).

## Suggested commit sequence
`T3.0 watchlist → T3.1 source_tagging → T3.2 tracker → T3.3 mempool_observer → T3.4 integration →
T3.5 dedupe → T3.6 live_smoke`.
Highest-risk: **T3.2/T3.3** (request lifecycle correctness under floods/disconnects) and **T3.4** (real
inv→getdata→tx→store).
