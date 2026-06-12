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
- `pending` — `%{txid => %{raw: raw_bin, announced_to: MapSet(peer), relayed_back: MapSet(peer),
  served_to: MapSet(peer), bar: non_neg_integer, propagated?: boolean, first_at_ms: t}}`. `raw_bin` is the
  **decoded binary tx bytes** (not the ASCII-hex string), served verbatim on a matching `getdata`.
  `served_to` records which peers we've already served this tx to (per-`(txid, peer)` `getdata` dedup — see
  Bounded resources). The map size is capped at `max_pending` (see below).
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
| `{:broadcast, txid, raw_bin, targets, bar}` | If `\|pending\| ≥ max_pending` → do **not** enqueue and signal `:saturated` (the `TxRelay` synchronous enqueue call surfaces this to `broadcast_tx/2` as `{:error, :saturated}` — §C); else record `pending[txid]` with `announced_to = targets` (the `N − held` set the `TxRelay` chose), `bar = held`, `served_to = ∅`. Normative 5-tuple shape. | `[{:send_inv, peer, txid} for peer <- targets]` or `[{:saturated, txid}]` |
| `{:getdata, txid, peer}` | If `pending[txid]` **and `peer ∉ served_to`** → serve the stored raw tx and add `peer` to `served_to`; a repeat `getdata` from the same peer for the same txid, or a non-pending txid, → ignore (per-`(txid, peer)` dedup). | `{:send_tx, peer, raw}` or none |
| `{:inv, txid, peer}` | If `pending[txid]` **and `peer ∉ announced_to`** → add to `relayed_back`; once `bar ≥ 1` and `\|relayed_back\| ≥ bar` and not already emitted → set `propagated?` and emit `:propagated`. A `peer ∈ announced_to` (target echo) is ignored. | `{:propagated, txid}` or none |
| `{:reject, txid, peer, reason}` | Record the rejection for that txid (surfaced to the broadcast caller/audit). | `{:rejected, txid, peer, reason}` |
| `:tick` | Drop `pending` entries older than `ttl_ms`, emitting `:unconfirmed` for each. | `[{:unconfirmed, txid}]` |

- **Hold-back rule (announce targets + propagation `bar`).** Of `N` live peers, **hold back**
  `held = min(2, N−1)` and **announce to the remaining `N − held`**. A relay-back **counts iff its peer is not
  in `announced_to`** — i.e. *any* non-target peer advertising the tx is propagation evidence (it learned the
  tx from the network, not from us); this includes the deliberately held-back peers and any peer that joined
  after we broadcast. The hold-back's *only* job is to guarantee that at broadcast time **at least `held`
  non-target peers exist**, so the bar is reachable — it is **not** an additional membership test on the
  relay-back (the reducer stores only `announced_to`; it does not pin a held-back pid set, which would be
  fragile under churn/late-joins). `bar = held`, and `:propagated` fires only when `bar ≥ 1` **and**
  `|relayed_back ∖ announced_to| ≥ bar`. A peer **in** `announced_to` (target echo) is never counted.
  Concretely:
  - **N ≥ 3** → announce to `N−2`, hold back 2, `bar = 2` (normal).
  - **N == 2** → announce to 1, hold back 1, `bar = 1` (degraded — logged).
  - **N == 1** → announce to the single peer, hold back 0; **propagation cannot be confirmed** (no
    non-target can exist), so the broadcast never reaches `:propagated` and terminates at `:unconfirmed`
    via the TTL tick. Logged — never silently claimed propagated.
  - **N == 0** → cold start: P2P is skipped entirely, RPC/REST only (§C).
  This keeps the core invariant in every mode: **a relay-back counts only if its peer is not in
  `announced_to`**, and `held ≥ 1` guarantees such a peer can exist before we ever claim propagation.
- **Idempotent propagation:** `:propagated` fires exactly once per txid (track an emitted flag).
- **Bounded resources (blocker — a network-facing relay must not grow unbounded).** Mirroring the Phase-3
  observer's flood controls:
  - `max_pending` (default 256) caps concurrent in-flight broadcasts; over the cap the synchronous enqueue
    returns `{:error, :saturated}` and `broadcast_tx/2` **falls back to RPC** (§C — the tx still broadcasts;
    status per the RPC result), never silently dropping it.
  - `max_tx_bytes` (**default 1_000_000**) caps a single retained tx; `byte_size(raw_bin) > max_tx_bytes` is
    rejected **upstream in `broadcast_tx/2`** (row `rejected`, `error: "transaction too large"`) so it is
    never stored or served by the relay. Together these bound retained raw bytes to `max_pending ×
    max_tx_bytes`.
  - **per-`(txid, peer)` `getdata` dedup** via `served_to`: we serve a given pending tx to a given peer **at
    most once**; repeat `getdata` from a hostile/looping peer is ignored (no repeated `send_tx` work).
  - **malformed/oversized inventories**: `inv`/`getdata` bodies are parsed with the Phase-3 `Inv.parse`
    `max_items` guard (`@max_inv_items`); a body exceeding it, or that fails to parse, is dropped — no
    unbounded parse work on unsolicited frames.
  These are configurable (`config :athanor, Athanor.P2P, relay: [max_pending: …, max_tx_bytes: …]`) and
  unit-tested in T4.0/T4.1/T4.2 (cap enforcement, oversized-`raw_bin` reject, `served_to` dedup,
  oversized-inv drop, no unbounded `pending` growth).
- **Target selection (blocker — fairness / no origin fingerprint).** Announce targets and held-back peers
  are chosen through an injected **`:selector`** seam — `select(pids, held) -> {targets, held_back}` — **not**
  by `Enum.take/2` over incidental `PeerRegistry.pids/1` (map) order. Production default **randomly shuffles**
  (`:rand`-based) so the announce/hold-back split varies per broadcast and does not leak a stable per-node
  origin pattern; tests inject a **deterministic** selector. T4.1/T4.3 assert selection goes through the seam
  (not registry order). Goal stated explicitly: target selection must not be a stable per-node fingerprint.

**Tests (T4.0).** Each row as a unit case over the pure reducer (inject `now_ms`), asserting the **normative
`{:broadcast, txid, raw_bin, targets, bar}` 5-tuple** and target vs non-target membership **explicitly** so a
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
- **once-only**: a repeat `inv` from an already-counted non-target peer (or any further `inv` after the
  threshold) does **not** re-emit `:propagated` (`propagated?` guards it);
- `reject` recorded; tick expires stale pending as `:unconfirmed`;
- **bounded resources:** at `max_pending`, a further `{:broadcast}` is dropped with `:saturated` (no
  `pending` growth); a repeat `getdata` from an already-served `(txid, peer)` yields **no** second `send_tx`;
  an oversized/malformed `inv`/`getdata` body is dropped.

---

## §C — Broadcast integration contract (`services/broadcast.ex`)

**Normative public API (blocker — arity).** On `main` the public entrypoint is `broadcast_tx(raw_tx_hex)`
(arity 1), called by `transaction_controller.ex:68` and `wallet_channel.ex:203`; there is **no** dependency
seam today. Phase 4 changes it to **`broadcast_tx(raw_tx_hex, opts \\ [])`** — a backward-compatible default
second arg, so the **two existing arity-1 callers keep working unchanged** (T4.2 asserts this) and the
audit-row + return shape are preserved. The new `opts` carry the test/runtime seams (both **new** in Phase 4;
neither exists on `main`):
- `:relay` — a **synchronous enqueue** seam `(txid, raw_bin -> :ok | {:error, :saturated})` (default: a
  `GenServer.call` to the supervised `TxRelay` that runs the capacity check + records `pending` and returns;
  the `inv`/`getdata`/`tx` frame sends + lifecycle stay async inside `TxRelay`). The call is synchronous
  **only** so the saturation verdict is available to `broadcast_tx/2` immediately (see saturation handling
  below); it does not wait on propagation;
- `:broadcaster` — `(raw_tx_hex -> {:ok, txid} | {:error, reason})` (default: `&RpcClient.send_raw_transaction/1`,
  i.e. exactly today's direct call), so tests exercise the RPC path without a node;
- `:rpc_fallback?` — `boolean` (**default `true`**; runtime default also overridable via
  `config :athanor, Athanor.P2P, rpc_fallback: true`). When `true`, the `:broadcaster` is invoked as the
  belt-and-suspenders fallback **even on the P2P-relay path**; when `false`, the `:broadcaster` is **not**
  called once the tx is relayed via P2P (≥1 live peer) — propagation must then be confirmed by the P2P
  relay-back alone. `:rpc_fallback?` only gates the *post-relay* fallback; the **cold-start path** (P2P
  disabled or zero peers) always calls `:broadcaster` regardless, so cold-start safety is never disabled.
This stays **one** public broadcast API; P2P-vs-RPC is a routing decision inside it. The broadcast path
becomes **P2P-primary, REST/RPC fallback**.

**Decode-once boundary + upstream validation (blockers — hex/wire bytes & the error path).**
`broadcast_tx/2` parses `raw_tx_hex` **exactly once** into a `%BSV.Transaction{}` (as it does today) and
derives `txid = Transaction.txid_binary/1` (the project's wire/internal byte order, as in Phase 3) and the
**decoded binary tx bytes** `raw_bin = Transaction.to_binary/1`. *All* validation happens here, synchronously,
before anything async:
- **Invalid hex / unparseable tx** → the audit row is written `status: "rejected"`,
  `error: "invalid raw transaction"`, **no relay enqueue, no `inv`, and no RPC fallback**.
- **Oversized tx** — `byte_size(raw_bin) > max_tx_bytes` (**default 1_000_000**, config
  `relay: [max_tx_bytes: …]`) → row `rejected`, `error: "transaction too large"`, **no relay enqueue, no
  `inv`, no RPC fallback**. Enforced **here, upstream**, so an over-limit tx is never handed to / stored by
  the relay — which is what makes the `max_pending × max_tx_bytes` retained-bytes bound actually hold.
- The relay `:relay` seam (synchronous enqueue) then either accepts (`:ok`) or reports
  **`{:error, :saturated}`** when `|pending| ≥ max_pending`. On accept, `TxRelay` announces `inv({:tx, txid})`
  and serves *those exact binary bytes* on `getdata` — never the ASCII-hex string. The relay never sees
  invalid/oversized input (rejected above), so the only error it can return is `:saturated`.

- When P2P is enabled and ≥1 live peer exists, call the `:relay` seam:
  - **`:ok`** (enqueued) → mark the row `status: "relayed"`; the RPC/REST `broadcaster` then runs as the
    **belt-and-suspenders fallback** (BSV nodes dedupe a tx already seen via P2P) **iff `:rpc_fallback?` is
    `true`** (the default); with `:rpc_fallback?: false` it is skipped (T4.3 isolates the propagation proof).
  - **`{:error, :saturated}`** → the relay declined (at capacity), so P2P can't carry the tx; `broadcast_tx/2`
    **logs the saturation and falls back to the `:broadcaster` exactly like the cold-start path** (always
    called, regardless of `:rpc_fallback?`), setting the row to `accepted`/`rejected` per the RPC result. The
    tx is therefore still broadcast; saturation degrades to RPC-only rather than dropping the tx, and the
    status reflects the authoritative RPC outcome (never silently `relayed`).
- When P2P is disabled or there are **zero** live peers (cold start), behavior is **exactly today's** —
  RPC/REST only. This is the cold-start-safety rule carried from DXS; it is the headline T4.2 test.

**Audit identity bridge (blocker — binary P2P txid vs text audit row).** `broadcasts.txid` stays the
existing **display-order hex string** (`Transaction.tx_id_hex/1`) — unchanged column, backward-compatible
return shape. `broadcast_tx/2` inserts the row keyed by `txid_hex` and hands the relay the **binary**
`txid_bin = Transaction.txid_binary/1` (the relay/`Tracker` key on binary internally, as in Phase 3). The
async audit callbacks (`:propagated`/`:unconfirmed`/`:rejected`) carry the **binary** txid; before touching
the row they convert it back to display hex via the Phase-3 `display_hex/1` reversal
(`txid_bin |> reverse-bytes |> Base.encode16(:lower)`) and look the row up by that hex key. So a binary relay
event updates **the exact row** `broadcast_tx/2` inserted, and no 32-byte binary is ever written into the
text column. T4.1/T4.2 assert this round-trip explicitly.

**Audit status model (single `status` string column; widen `Broadcast.changeset`, no PG-enum so no
migration — confirm in T4.2).** The complete **monotonic, never-downgrading lattice** — `advance_status/2`
sets `status` to the higher-ranked of the current and incoming value, never the lower:

| rank | status | set by |
|---|---|---|
| 0 | `pending` | row created |
| 1 | `relayed` | announced to ≥1 peer via P2P |
| 2 | `unconfirmed` | TTL tick with no propagation |
| 3 | `rejected` | a node RPC error **or** a P2P peer `reject` |
| 4 | `accepted` | the RPC/REST `:broadcaster` returned ok (authoritative single-node acceptance) |
| 5 | `propagated` | ≥`bar` distinct non-target peers re-announced it (network acceptance; the ceiling) |

This resolves the two terminal-below-accepted cases deterministically: **`rejected` (3) outranks
`unconfirmed` (2)**, so a late peer/RPC `reject` **does** overwrite an `unconfirmed` row (more informative),
while a TTL `:unconfirmed` **never** overwrites an existing `rejected` (or `accepted`/`propagated`). A node
`accepted` (4) outranks a single peer's `rejected` (3). `accepted`/`propagated` are never downgraded.
Cold-start (no P2P) collapses to exactly today's `pending → accepted | rejected`. Updates are out-of-band:
`broadcast_tx/2` returns after recording + handing off; async events apply via `advance_status/2`.

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
audit callbacks; `broadcast(txid, raw_bin)` receives the **already-validated decoded binary tx bytes** and
the derived `txid` from `broadcast_tx/2` (validation is upstream — §C; the relay is fire-and-forget and never
returns a parse error), reads `PeerRegistry.pids/1`, picks the announce/hold-back split through the injected
**`:selector`** seam (§B — default random shuffle; deterministic in tests; `bar = min(2, N−1)`), and steps
`{:broadcast, txid, raw_bin, targets, bar}`. `:tick`/TTL and the `:max_pending`/`:selector` opts injected.
**RED:** `tx_relay_test.exs` (fake peers + stub audit sink): `broadcast(txid, raw_bin)` → `send_inv`
carrying `{:tx, txid}` to the targets the **`:selector`** chose (assert selection goes through the seam, not
`PeerRegistry.pids/1` order); `getdata` for a pending txid → `send_tx` with the **exact binary bytes**
`raw_bin` (which hash to the announced txid — assert wire bytes, not ASCII hex); a relay-back from **any**
peer ∉ `announced_to` counts toward `bar` while a peer ∈ `announced_to` (target echo) does not; `bar`
distinct non-target `inv`s → `:propagated` audit; `reject` → audit; non-pending `getdata` ignored; **bounded
resources** — at `max_pending` the synchronous enqueue returns `{:error, :saturated}`, a repeat `getdata` from an already-served
`(txid, peer)` is **not** re-served, an oversized `inv` body is dropped.
**GREEN/REFACTOR:** as above; reuse the Phase-3 `apply_actions` shape.

### T4.2 — Broadcast integration (§C) — `services/broadcast.ex` + `Athanor.Schema.Broadcast`
**Status-contract work (blocker).** `Broadcast.changeset/2` today validates only `pending|accepted|rejected`
(`lib/athanor/schema/broadcast.ex`). Widen `validate_inclusion` to add `relayed|propagated|unconfirmed` and
update the schema moduledoc. `status` is a plain string column (no PG enum), so **no migration is required —
T4.2 must confirm this** (a `mix ecto` check); add one only if the column is constrained.
**RED:** `broadcast_test.exs` additions —
- each new status (`relayed`/`propagated`/`unconfirmed`) **persists through `Broadcast.changeset/2`**;
- **backward-compat arity:** `broadcast_tx(raw_tx_hex)` (arity 1, the existing controller/channel callers)
  still works and behaves exactly as today when no `opts` are given;
- P2P-enabled + live peers, **default `:rpc_fallback?` (`true`)** → relay invoked + row `relayed`, **and the
  `:broadcaster` is still called** (belt-and-suspenders); a separate case with `:rpc_fallback?: false` →
  relay invoked but `:broadcaster` **not** called;
- **P2P disabled / zero peers → RPC-only, row exactly as today** (cold-start safety), via the default
  `:broadcaster` (`&RpcClient.send_raw_transaction/1`);
- **lattice precedence (monotonic, never downgrades)** — `pending<relayed<unconfirmed<rejected<accepted<propagated`:
  `relayed` + RPC-accept → `accepted`; later `:propagated` → `propagated`; **reject-then-TTL**: a `rejected`
  row is **not** overwritten by a TTL `:unconfirmed`; **TTL-then-reject**: an `unconfirmed` row **is**
  overwritten by a later `:reject` (rank 3 > 2); a single peer `reject` does **not** override `accepted`;
  `accepted`/`propagated` are never downgraded;
- **audit identity bridge:** a **binary**-keyed `:propagated` (and `:unconfirmed`/`:rejected`) event updates
  the **exact** row `broadcast_tx/2` inserted (keyed by display-hex), and the row's `txid` stays display-hex
  (backward-compatible); no 32-byte binary is written to the text column;
- **invalid `raw`** → row `rejected` (`error: "invalid raw transaction"`), **no** `inv`, **no** RPC call;
- **oversized `raw`** (`> max_tx_bytes`) → row `rejected` (`error: "transaction too large"`), **no** relay,
  **no** `inv`, **no** RPC call (not stored/served);
- **relay saturated** (`:relay` returns `{:error, :saturated}`) → `broadcast_tx/2` falls back to the
  `:broadcaster` (always, like cold-start) and the row reflects the RPC result (`accepted`/`rejected`),
  **not** `relayed`; tested with and without `:rpc_fallback?`.
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
**The RPC fallback is opted out via `:rpc_fallback?: false`** with a `:broadcaster` stub that flunks if called, and the
test **asserts it was never called** — so `:propagated` is proven to come through the P2P announce→relay-back
path, not the node/RPC path. (The belt-and-suspenders fallback behavior is covered separately in T4.2.)

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
- The §A fan-out keeps Phase 2/3 behavior unchanged for single-sink/`nil` configs.
- **One broadcast public API:** `broadcast_tx(raw_tx_hex, opts \\ [])` — the existing arity-1 callers
  (`transaction_controller`, `wallet_channel`) work unchanged (asserted); `:relay`/`:broadcaster` are
  Phase-4-new injected seams, both defaulting to today's behavior.
- **Normative event shape:** the reducer and `TxRelay` agree on `{:broadcast, txid, raw_bin, targets, bar}`
  (`bar` stored in `pending`, not re-derived); T4.0/T4.1 assert it for N≥3, N==2, N==1, and once-only
  propagation.
- **Outbound consistency:** `broadcast_tx/2` decodes `raw_tx_hex` once into `%BSV.Transaction{}`, deriving
  `txid = Transaction.txid_binary/1` (wire/internal order) and binary `raw_bin = Transaction.to_binary/1`; the
  relay announces `inv({:tx, txid})` and serves `raw_bin` (binary wire bytes, never ASCII hex). Validation is
  upstream: invalid hex/unparseable tx → row `rejected` (`error: "invalid raw transaction"`) with **no** relay,
  **no** `inv`, **no** RPC call — tested for valid and invalid input. The relay seam `(txid, raw_bin -> :ok)`
  is fire-and-forget (never returns a parse error).
- **Fallback control contract:** `:rpc_fallback?` (default `true`; config `rpc_fallback`) gates the
  *post-relay* belt-and-suspenders `:broadcaster` call only — the cold-start path always calls it. T4.2
  asserts default-`true` calls `:broadcaster` on the live-peer path and `false` skips it; T4.3 sets `false`.
- **Propagation proof isolation:** the T4.3 round-trip sets `:rpc_fallback?: false` (with a `:broadcaster`
  stub that flunks if called) and asserts it is **never called**, so `:propagated` is proven via the P2P
  announce→non-target-relay-back path, not the node path. Any peer ∉ `announced_to` counts (hold-back only
  guarantees ≥`bar` non-targets exist).
- **Audit status contract:** `Broadcast.changeset/2` accepts `pending|relayed|unconfirmed|rejected|accepted|
  propagated`, all persisting; `advance_status/2` follows the monotonic lattice
  `pending(0)<relayed(1)<unconfirmed(2)<rejected(3)<accepted(4)<propagated(5)`, never downgrading
  (reject-then-TTL keeps `rejected`; TTL-then-reject becomes `rejected`); no migration (string column —
  confirmed in T4.2). The audit row's `txid` stays display-order hex; binary relay events are converted via
  `display_hex/1` before row lookup, so they update the exact `broadcast_tx/2` row (asserted).
- **Bounded resources:** `max_pending` (default 256; over-cap → synchronous `{:error, :saturated}` →
  `broadcast_tx/2` falls back to RPC, tx still broadcast), `max_tx_bytes` (default 1_000_000; oversized
  `raw_bin` rejected upstream, never stored/served — so retained bytes ≤ `max_pending × max_tx_bytes`),
  per-`(txid, peer)` `getdata` dedup via `served_to`, and the `Inv.parse` `max_items` guard on
  `inv`/`getdata`; T4.0/T4.1/T4.2 prove the caps and no unbounded `pending` growth.
- **Target selection:** the announce/hold-back split goes through an injected `:selector` (random shuffle in
  prod — no stable origin fingerprint; deterministic in tests); T4.1/T4.3 assert selection uses the seam, not
  `PeerRegistry.pids/1` order.
- Format clean under Elixir 1.15; app code clean under `mix compile --warnings-as-errors`.

---

## Suggested commit sequence
`feat(p2p): frame-sink fan-out + PeerRegistry.pids (T4.S)` → `feat(p2p): pure TxRelay tracker (T4.0)` →
`feat(p2p): TxRelay GenServer (T4.1)` → `feat(p2p): P2P-primary broadcast with RPC fallback (T4.2)` →
`test(p2p): self-broadcast round-trip integration (T4.3)` → `test(p2p): relay live smoke (T4.4)`.
