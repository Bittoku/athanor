# Phase 4 — Broadcast + relay-back (TDD task breakdown)

Phase 3 landed inbound mempool ingest (`inv → getdata → tx → index`). Phase 4 adds the **outbound**
direction: announce our own transactions to the peer set, serve the `getdata` they answer with, and
confirm propagation by watching the tx come *back* as an `inv` from a peer we did **not** send it to.
Then fold this in front of the existing RPC/REST broadcast as the **primary** path, with the node/REST
broadcast as an honest fallback.

This doc specifies the contracts that must be settled **before** implementation (the architectural one is
§A — the pool currently has a single `frame_sink`, and Phase 4 introduces a *second* frame consumer), then
the bottom-up TDD tasks. Same shape as Phase 1–3: pure reducers decide, one thin GenServer does IO; inject
time/timers/transport; no `Process.sleep`/`Process.alive?`; commit `feat(p2p): <task>` (no AI attribution);
format under the project's Elixir **1.15** toolchain.

---

## §A — Frame routing fan-out + live-peer seam (settle before T4.1)

**The problem.** The pool forwards each post-handshake application frame to **one** `frame_sink`
(`peer_pool.ex:91-93`), which Phase 3 wired to the `MempoolObserver`. Phase 4's `TxRelay` is a *second*
consumer that needs `getdata` (to serve our pending tx), `inv` (to detect our tx relayed back), and
`reject` (to record a peer's refusal). The observer needs `tx`/`inv`. So a single sink no longer suffices.

**Decision (recommended): `frame_sink` becomes a fan-out list.**
- `PeerPool.Config.frame_sink` accepts `pid | atom | [pid | atom] | nil`. When a list, the pool forwards
  the `{:peer, pid, :frame, frame}` message to **each** sink (order-independent); a single sink and `nil`
  keep their current behavior exactly (back-compat — Phase 2/3 tests unchanged).
- `P2P.Supervisor` wires `frame_sink: [MempoolObserver, TxRelay]` and starts `TxRelay` as a supervised,
  registered child (Registry → Observer → **TxRelay** → Pool under `:rest_for_one`, so both sink names are
  registered before the pool forwards).
- Each consumer ignores commands it doesn't own (the observer already drops non-`inv`/`tx`/`notfound`;
  the relay drops everything except `getdata`/`inv`/`reject`). Both legitimately see `inv` — disjoint
  concerns (observer: *should I fetch this?*; relay: *is this my tx coming back?*).

> Rejected alternatives: (1) a dedicated `FrameRouter` GenServer that dispatches by command — extra hop and
> state for no gain over a list; (2) `Phoenix.PubSub` per-frame — heavier and loses the originating pid
> cleanly. The list keeps the pool the lifecycle owner and the Phase-1 owner-message shape intact.

**Live-peer seam.** The relay must announce to *N−2* live peers and know their pids. `PeerRegistry` tracks
`by_addr`/`by_pid` but exposes only `addresses/1`. Add **`PeerRegistry.pids/1`** returning the live peer
pids (the `by_addr` values), so the relay selects announce targets without reaching into pool internals.

**Tests.** Pool fans a frame out to every sink in a list (assert all receive it); a single-pid/`nil` sink is
unchanged. `PeerRegistry.pids/1` returns exactly the registered live pids.

---

## §B — Relay lifecycle contract (pure `TxRelay.Tracker`)

A pure reducer governs one broadcast's life with no process/IO, mirroring the Phase-3 `MempoolObserver.Tracker`.

**State** (per in-flight broadcast, keyed by txid — wire/internal order, as in Phase 3):
- `pending` — `%{txid => %{raw: payload, announced_to: MapSet(peer), relayed_back: MapSet(peer),
  first_at_ms: t}}`. `raw` is served on a matching `getdata`.
- A broadcast is **propagated** once `relayed_back` contains **≥2 distinct peers that were NOT in
  `announced_to`** — i.e. peers we didn't tell are now advertising it, proving the network accepted and
  re-gossiped it. (Counting only non-targets avoids mistaking our own announce echoing.)
- `ttl_ms` / `:tick` expiry: a broadcast that never reaches the propagation bar within the window is
  marked `:unconfirmed` and dropped from `pending` (the caller already has the REST/RPC fallback result).

**`step(state, event, now_ms) -> {state, actions}`** — events and locked decisions:

| Event | Decision | Action(s) |
|---|---|---|
| `{:broadcast, txid, raw, targets}` | Record `pending[txid]` with `announced_to = targets`. | `[{:send_inv, peer, txid} for peer <- targets]` |
| `{:getdata, txid, peer}` | If `pending[txid]` → serve the stored raw tx to that peer; else ignore (not ours). | `{:send_tx, peer, raw}` or none |
| `{:inv, txid, peer}` | If `pending[txid]` and `peer ∉ announced_to` → add to `relayed_back`; on crossing **≥2** → emit `:propagated` once. | `{:propagated, txid}` or none |
| `{:reject, txid, peer, reason}` | Record the rejection for that txid (surfaced to the broadcast caller/audit). | `{:rejected, txid, peer, reason}` |
| `:tick` | Drop `pending` entries older than `ttl_ms`, emitting `:unconfirmed` for each. | `[{:unconfirmed, txid}]` |

- **Who we announce to:** `N−2` live peers (leave headroom; the 2 we hold back are where we *expect* the
  relay-back from — they should learn it from the network, not from us). If fewer than 3 live peers, announce
  to all and **lower the propagation bar to ≥1 non-target** (documented degradation, `log/0`-ged — never a
  silent cap).
- **Idempotent propagation:** `:propagated` fires exactly once per txid (track an emitted flag).

**Tests (T4.0).** Each row as a unit case over the pure reducer (inject `now_ms`): announce→inv targets;
getdata-serves-only-ours; relayed-back-by-non-target counts, by-a-target does **not**; `:propagated` fires
once at the 2nd distinct non-target; reject recorded; tick expires stale pending as `:unconfirmed`; the
<3-peer degraded bar.

---

## §C — Broadcast integration contract (`services/broadcast.ex`)

`broadcast_tx/2` keeps its audit-row + return shape. The broadcast path becomes **P2P-primary, REST/RPC
fallback**, behind an injected seam so tests need no node:
- New optional opt `:relay` — `(txid, raw -> :ok)` (default: cast to the supervised `TxRelay`). When P2P is
  enabled and ≥1 live peer exists, announce via the relay and mark the audit row `status: "relayed"`; the
  RPC/REST `broadcaster` still runs as the **belt-and-suspenders fallback** (BSV nodes dedupe a tx they
  already saw via P2P, so double-submit is safe) **unless** config opts out.
- When P2P is disabled or there are **zero** live peers (cold start), behavior is **exactly today's** —
  RPC/REST only. This is the cold-start-safety rule carried from DXS; it is the headline T4.2 test.
- Propagation is asynchronous: `broadcast_tx` returns after recording + handing off; `:propagated` /
  `:unconfirmed` / `:rejected` update the audit row out of band (a `Broadcast.status` transition:
  `pending → relayed → propagated | unconfirmed`, and `rejected` on a `reject`).

**`matches?`-style single authority:** there is one broadcast entry point (`broadcast_tx/2`); P2P vs RPC is a
*routing* decision inside it, not a second public API.

---

## Tasks (bottom-up, each independently verifiable)

### T4.S — Frame fan-out + registry pids seam (§A) — do FIRST
**RED:** pool with `frame_sink: [a, b]` forwards a post-handshake frame to both; `frame_sink: pid` and
`nil` unchanged. `PeerRegistry.pids/1` returns the live pids.
**GREEN:** make the pool's forward iterate a normalized sink list; add `PeerRegistry.pids/1`.
**REFACTOR:** normalize `frame_sink` to a list once at init; keep the single-sink/`nil` fast paths.

### T4.0 — Pure relay reducer (§B) — `Athanor.P2P.TxRelay.Tracker`
**RED:** `tracker_test.exs` — every §B row, time injected.
**GREEN:** the reducer (pending map, non-target relay-back counting, once-only `:propagated`, tick TTL).
**REFACTOR:** doc header; share the txid-order convention with Phase 3.

### T4.1 — `TxRelay` GenServer — `Athanor.P2P.TxRelay`
The thin shell: registered `frame_sink` member; on `{:peer,_,:frame,%Frame{command:"getdata"|"inv"|"reject"}}`
folds `Tracker.step` and performs `{:send_tx,_}`/`{:send_inv,_}`/`{:propagated,_}` via `Peer.send_frame` and
audit callbacks; `broadcast(txid, raw)` selects `N−2` targets from `PeerRegistry.pids/1` and steps
`{:broadcast,…}`; `:tick`/TTL timers injected.
**RED:** `tx_relay_test.exs` (fake peers + stub audit sink): broadcast → `send_inv` to N−2 captured;
`getdata` for a pending txid → `send_tx` with the stored bytes; non-target `inv` ×2 → `:propagated` audit;
`reject` → audit; non-pending `getdata` ignored.
**GREEN/REFACTOR:** as above; reuse the Phase-3 `apply_actions` shape.

### T4.2 — Broadcast integration (§C) — `services/broadcast.ex`
**RED:** `broadcast_test.exs` additions — P2P-enabled + live peers → relay invoked + row `relayed`
(+ fallback broadcaster still called); **P2P disabled / zero peers → RPC-only, row exactly as today**
(cold-start safety); a `:propagated` event transitions the row.
**GREEN:** inject `:relay`; route by `Supervisor.enabled?/0` + `PeerRegistry.pids/1 != []`.

### T4.3 — Integration: self-broadcast round-trips (real socket) — `tx_relay/integration_test.exs` (`async: false`)
`FakePeerServer` extended to **echo an inv back** for a tx it received via `getdata` (a non-target relaying
our tx). End-to-end over loopback through the real `P2P.Supervisor`: `broadcast_tx` → `inv` to peers →
server `getdata` → we serve `tx` → server (acting as a non-target) sends `inv` back → relay marks
`:propagated` → audit row `propagated`.

### T4.4 — Live smoke (`@tag :external`, testnet, CI-skipped) — `tx_relay/live_smoke_test.exs`
Broadcast a real (funded) tx on testnet and assert our own txid comes back via a non-target peer within a
bounded window (or, with no spendable input, assert the `inv → getdata → tx` serve completes for a crafted
tx). `mix test --only external`; mainnet opt-in via `P2P_SMOKE_NETWORK=mainnet`.

---

## Definition of Done (Phase 4)
- T4.S, T4.0–T4.3 green; `mix test test/athanor/p2p` + the broadcast tests clean (T4.4 excluded by default).
- No `Process.sleep`/`Process.alive?`; `now_ms`/timers injected.
- A self-broadcast tx round-trips over real sockets (seen back via a non-target peer); propagation requires
  **≥2 distinct non-target** relays (≥1 in the documented <3-peer degraded mode, logged).
- Cold-start safety: with P2P primary **and zero peers**, `broadcast_tx` still works via RPC/REST fallback,
  byte-for-byte as today.
- The §A fan-out keeps Phase 2/3 behavior unchanged for single-sink/`nil` configs; one broadcast public API.
- Format clean under Elixir 1.15; app code clean under `mix compile --warnings-as-errors`.

---

## Suggested commit sequence
`feat(p2p): frame-sink fan-out + PeerRegistry.pids (T4.S)` → `feat(p2p): pure TxRelay tracker (T4.0)` →
`feat(p2p): TxRelay GenServer (T4.1)` → `feat(p2p): P2P-primary broadcast with RPC fallback (T4.2)` →
`test(p2p): self-broadcast round-trip integration (T4.3)` → `test(p2p): relay live smoke (T4.4)`.
