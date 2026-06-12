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

**Live-peer seam.** The relay must announce to a subset of live peers (the §B hold-back rule) and know their pids. `PeerRegistry` tracks
`by_addr`/`by_pid` but exposes only `addresses/1`. Add **`PeerRegistry.pids/1`** returning the live peer
pids (the `by_addr` values), so the relay selects announce targets without reaching into pool internals.

**Tests.** Pool fans a frame out to every sink in a list (assert all receive it); a single-pid/`nil` sink is
unchanged. `PeerRegistry.pids/1` returns exactly the registered live pids.

---

## §B — Relay lifecycle contract (pure `TxRelay.Tracker`)

A pure reducer governs one broadcast's life with no process/IO, mirroring the Phase-3 `MempoolObserver.Tracker`.

**State** (per in-flight broadcast, keyed by txid — wire/internal order, as in Phase 3):
- `pending` — `%{txid => %{raw: payload, announced_to: MapSet(peer), relayed_back: MapSet(peer),
  bar: non_neg_integer, propagated?: boolean, first_at_ms: t}}`. `raw` is served on a matching `getdata`.
  **`bar` is supplied by the caller** in the `{:broadcast, …, bar}` event (the `TxRelay` computes it from the
  live-peer count via the hold-back rule — see below) and stored here; it is **not** derivable from
  `announced_to` alone (1 target ⇒ N=2/bar=1 *or* N=3/bar=2), which is why it is a first-class event field.
- A broadcast is **propagated** once `relayed_back` contains **≥`bar` distinct peers that were NOT in
  `announced_to`** (and `bar ≥ 1`) — i.e. peers we didn't tell are now advertising it, proving the network
  accepted and re-gossiped it. Counting **only non-targets** avoids mistaking our own announce echoing; a
  relay-back from a peer in `announced_to` is **never** counted. `propagated?` makes `:propagated`
  fire exactly once.
- `ttl_ms` / `:tick` expiry: a broadcast that never reaches the propagation bar within the window is
  marked `:unconfirmed` and dropped from `pending` (the caller already has the REST/RPC fallback result).

**`step(state, event, now_ms) -> {state, actions}`** — events and locked decisions:

| Event | Decision | Action(s) |
|---|---|---|
| `{:broadcast, txid, raw, targets, bar}` | Record `pending[txid]` with `announced_to = targets` (the `N − held` announce set the `TxRelay` chose) and `bar = held`. The **normative broadcast event shape** is this 5-tuple. | `[{:send_inv, peer, txid} for peer <- targets]` |
| `{:getdata, txid, peer}` | If `pending[txid]` → serve the stored raw tx to that peer; else ignore (not ours). | `{:send_tx, peer, raw}` or none |
| `{:inv, txid, peer}` | If `pending[txid]` **and `peer ∉ announced_to`** → add to `relayed_back`; once `bar ≥ 1` and `\|relayed_back\| ≥ bar` and not already emitted → set `propagated?` and emit `:propagated`. A `peer ∈ announced_to` (target echo) is ignored. | `{:propagated, txid}` or none |
| `{:reject, txid, peer, reason}` | Record the rejection for that txid (surfaced to the broadcast caller/audit). | `{:rejected, txid, peer, reason}` |
| `:tick` | Drop `pending` entries older than `ttl_ms`, emitting `:unconfirmed` for each. | `[{:unconfirmed, txid}]` |

- **Hold-back rule (announce targets + propagation `bar`).** Of `N` live peers, **hold back**
  `held = min(2, N−1)` and **announce to the remaining `N − held`**. The held-back peers are *where we expect
  the relay-back from* — they should learn the tx from the network, not from us — so they are the only peers
  whose `inv` can confirm propagation. `bar = held`, and `:propagated` fires only when `held ≥ 1` **and**
  `|relayed_back ∖ announced_to| ≥ bar`. By construction `relayed_back ∖ announced_to ⊆ held-back`, so a
  target echo can never satisfy the bar. Concretely:
  - **N ≥ 3** → announce to `N−2`, hold back 2, `bar = 2` (normal).
  - **N == 2** → announce to 1, hold back 1, `bar = 1` (degraded — logged).
  - **N == 1** → announce to the single peer, hold back 0; **propagation cannot be confirmed** (no
    non-target can exist), so the broadcast never reaches `:propagated` and terminates at `:unconfirmed`
    via the TTL tick. Logged — never silently claimed propagated.
  - **N == 0** → cold start: P2P is skipped entirely, RPC/REST only (§C).
  This keeps the core invariant in every mode: **a relay-back counts only if its peer is not in
  `announced_to`**, and `held ≥ 1` guarantees such a peer can exist before we ever claim propagation.
- **Idempotent propagation:** `:propagated` fires exactly once per txid (track an emitted flag).

**Tests (T4.0).** Each row as a unit case over the pure reducer (inject `now_ms`), asserting the **normative
`{:broadcast, txid, raw, targets, bar}` 5-tuple** and target vs non-target membership **explicitly** so a
target echo can never satisfy the bar:
- `{:broadcast, …, bar}` records `pending[txid]` with `announced_to = targets`, the stored `bar`, and
  `propagated? = false`;
- announce → `:send_inv` to exactly the `targets`;
- `getdata` serves only a pending (ours) txid, never a stranger's;
- a relay-back from a peer **in** `announced_to` does **not** count; from a held-back peer it does;
- **N ≥ 3 (`bar = 2`)**: `:propagated` fires once, only at the 2nd distinct *held-back* relay-back (a
  target echo + one held-back relay-back is **not** enough);
- **N == 2 (`bar = 1`)**: `:propagated` fires at the 1st held-back relay-back;
- **N == 1 (`held = 0`/`bar = 0`)**: no relay-back can count → never `:propagated`; the tick expires it to
  `:unconfirmed`;
- **once-only**: a repeat `inv` from an already-counted held-back peer (or any further `inv` after the
  threshold) does **not** re-emit `:propagated` (`propagated?` guards it);
- `reject` recorded; tick expires stale pending as `:unconfirmed`.

---

## §C — Broadcast integration contract (`services/broadcast.ex`)

`broadcast_tx/2` keeps its audit-row + return shape. The broadcast path becomes **P2P-primary, REST/RPC
fallback**, behind an injected seam so tests need no node:
- New optional opt `:relay` — **`(raw -> :ok)`** (default: cast to the supervised `TxRelay`). When P2P is
  enabled and ≥1 live peer exists, announce via the relay and mark the audit row `status: "relayed"`; the
  RPC/REST `broadcaster` still runs as the **belt-and-suspenders fallback** (BSV nodes dedupe a tx they
  already saw via P2P, so double-submit is safe) **unless** config opts out.
- When P2P is disabled or there are **zero** live peers (cold start), behavior is **exactly today's** —
  RPC/REST only. This is the cold-start-safety rule carried from DXS; it is the headline T4.2 test.

**txid is derived from `raw`, never trusted from the caller (blocker — outbound consistency).** The
`TxRelay` computes `txid = SHA256d(raw)` itself and uses *that* both as the `inv` it announces and as the
`pending` key it serves `getdata` from — so the announced txid and the served bytes are consistent by
construction (no "announce X, serve Y" class of bug). `broadcast_tx/2` already parses `raw` to derive the
audit txid; that derived value is the single source of truth. If `raw` does not parse as a transaction, it
is **not** announced or enqueued: the audit row is written `status: "rejected"` with an explicit
`error: "invalid raw transaction"`, and the RPC fallback is not attempted. The relay/tracker therefore
never receive a separately-supplied txid that could disagree with `raw`.

**Audit status model (single `status` string column; widen `Broadcast.changeset`, no PG-enum so no
migration — confirm in T4.2).** Statuses and the **monotonic, never-downgrading** precedence
`pending(0) < relayed(1) < accepted(2) < propagated(3)`:
- `pending` — row created.
- `relayed` — announced to ≥1 peer via P2P.
- `accepted` — the RPC/REST fallback returned ok (authoritative *single-node* acceptance). Coexists with
  the P2P axis as a strictly-higher tier than `relayed`: a tx that is `relayed` then RPC-`accepted` ends
  `accepted` unless it also reaches `propagated`.
- `propagated` — ≥`bar` distinct non-target peers re-announced it (network-level acceptance; the ceiling).
- `unconfirmed` — set **only** by the TTL tick on a row still `< accepted` (neither node-accepted nor
  propagated); it never overrides `accepted`/`propagated`.
- `rejected` — a node/peer refusal (RPC error, or a P2P `reject`) on a row still `< accepted`.
Cold-start (no P2P) collapses to exactly today's `pending → accepted | rejected`. Propagation is
asynchronous: `broadcast_tx/2` returns after recording + handing off; `:propagated`/`:unconfirmed`/
`:rejected` update the row out of band under this precedence (a transition never lowers the tier).

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
audit callbacks; `broadcast(raw)` **derives `txid = SHA256d(raw)`** (single source of truth — never a
caller-supplied txid), reads `PeerRegistry.pids/1`, applies the §B hold-back rule (announce to
`N − min(2, N−1)`, `bar = min(2, N−1)`), and steps `{:broadcast, txid, raw, targets, bar}`. Unparseable
`raw` is **not** enqueued or announced — it returns a deterministic error for the caller's audit path.
`:tick`/TTL timers injected.
**RED:** `tx_relay_test.exs` (fake peers + stub audit sink): `broadcast(raw)` → `send_inv` carrying the
**derived** txid to the `N − held` announce targets captured; an **unparseable `raw`** → no `send_inv`,
deterministic error; `getdata` for a pending txid → `send_tx` with the exact stored bytes (which hash to the
announced txid); `bar` distinct held-back `inv`s → `:propagated` audit; `reject` → audit; non-pending
`getdata` ignored.
**GREEN/REFACTOR:** as above; reuse the Phase-3 `apply_actions` shape.

### T4.2 — Broadcast integration (§C) — `services/broadcast.ex` + `Athanor.Schema.Broadcast`
**Status-contract work (blocker).** `Broadcast.changeset/2` today validates only `pending|accepted|rejected`
(`lib/athanor/schema/broadcast.ex`). Widen `validate_inclusion` to add `relayed|propagated|unconfirmed` and
update the schema moduledoc. `status` is a plain string column (no PG enum), so **no migration is required —
T4.2 must confirm this** (a `mix ecto` check); add one only if the column is constrained.
**RED:** `broadcast_test.exs` additions —
- each new status (`relayed`/`propagated`/`unconfirmed`) **persists through `Broadcast.changeset/2`**;
- P2P-enabled + live peers → relay invoked + row `relayed` (+ the RPC fallback still called);
- **P2P disabled / zero peers → RPC-only, row exactly as today** (cold-start safety);
- **precedence (monotonic, never downgrades):** a `relayed` row that the RPC fallback then accepts ends
  `accepted`; a later `:propagated` lifts it to `propagated`; a TTL `:unconfirmed` does **not** override an
  `accepted`/`propagated` row; a `:propagated`/`:unconfirmed`/`:rejected` event applies via the precedence;
- **invalid `raw`** → row `rejected` with `error: "invalid raw transaction"`, **no** `inv`, **no** RPC call.
**GREEN:** widen the changeset; inject `:relay`; route by `Supervisor.enabled?/0` + `PeerRegistry.pids/1 != []`;
apply the status precedence in one place (a `Broadcast.advance_status/2`-style helper) so out-of-band events
never lower the tier.

### T4.3 — Integration: self-broadcast round-trips (real socket) — `tx_relay/integration_test.exs` (`async: false`)
End-to-end over loopback through the real `P2P.Supervisor` with **≥3** `FakePeerServer`s so the hold-back
rule yields real non-targets (announce to `N−2`, hold back 2). `FakePeerServer` is extended to **send an
`inv(our_txid)` back** (simulating a peer that learned the tx from the network), configured per-server.
`broadcast_tx` → `inv` to the announce targets → a target `getdata` → we serve `tx`; meanwhile the two
**held-back** servers send `inv(our_txid)` → the relay counts those (and **ignores** any echo from the
announce target) → marks `:propagated` → audit row `propagated`. The test asserts the relay-back peers were
**not** in `announced_to` (so target echo cannot be what flips the status).

### T4.4 — Live smoke (`@tag :external`, testnet, CI-skipped) — `tx_relay/live_smoke_test.exs`
Broadcast a real (funded) tx on testnet and assert our own txid comes back via a non-target peer within a
bounded window (or, with no spendable input, assert the `inv → getdata → tx` serve completes for a crafted
tx). `mix test --only external`; mainnet opt-in via `P2P_SMOKE_NETWORK=mainnet`.

---

## Definition of Done (Phase 4)
- T4.S, T4.0–T4.3 green; `mix test test/athanor/p2p` + the broadcast tests clean (T4.4 excluded by default).
- No `Process.sleep`/`Process.alive?`; `now_ms`/timers injected.
- A self-broadcast tx round-trips over real sockets (seen back via a **held-back, non-target** peer);
  propagation requires **≥`bar` distinct non-target** relays where `bar = held = min(2, N−1)` (2 normally,
  1 with exactly 2 peers, and **never claimed** with 1 peer — it expires `:unconfirmed`). Tests assert the
  relay-back peer ∉ `announced_to`, so a target echo can never flip the status.
- Cold-start safety: with P2P primary **and zero peers**, `broadcast_tx` still works via RPC/REST fallback,
  byte-for-byte as today.
- The §A fan-out keeps Phase 2/3 behavior unchanged for single-sink/`nil` configs; one broadcast public API.
- **Normative event shape:** the reducer and `TxRelay` agree on `{:broadcast, txid, raw, targets, bar}`
  (`bar` stored in `pending`, not re-derived); T4.0/T4.1 assert it for N≥3, N==2, N==1, and once-only
  propagation.
- **Outbound consistency:** `txid` is derived from `raw` (`SHA256d`), used for both the announced `inv` and
  the served `tx`; unparseable `raw` is rejected (`error: "invalid raw transaction"`) with no `inv` and no
  RPC call — tested for valid and invalid `raw`.
- **Audit status contract:** `Broadcast.changeset/2` accepts `pending|relayed|propagated|unconfirmed|
  accepted|rejected`, all persisting; transitions follow the monotonic precedence
  `pending<relayed<accepted<propagated` (with `unconfirmed`/`rejected` only below `accepted`); no migration
  required (string column — confirmed in T4.2).
- Format clean under Elixir 1.15; app code clean under `mix compile --warnings-as-errors`.

---

## Suggested commit sequence
`feat(p2p): frame-sink fan-out + PeerRegistry.pids (T4.S)` → `feat(p2p): pure TxRelay tracker (T4.0)` →
`feat(p2p): TxRelay GenServer (T4.1)` → `feat(p2p): P2P-primary broadcast with RPC fallback (T4.2)` →
`test(p2p): self-broadcast round-trip integration (T4.3)` → `test(p2p): relay live smoke (T4.4)`.
