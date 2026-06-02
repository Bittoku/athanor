# Phase 1 — Single Peer + Handshake: TDD Task Breakdown

Companion to `thin-node-p2p-plan.md` and `thin-node-p2p-phase0-tasks.md`.
Scope: **one** outbound TCP connection — connect, complete the BSV handshake, run a steady-state
receive loop (ping/pong, frame dispatch), and tear down cleanly so a supervisor can restart it.
Depends on Phase 0 codec being green. No pool, no discovery, no ingest (those are Phase 2–3).

## Core TDD strategy for networked code
The handshake/IO logic is split into three testable layers so we never need to mock `:gen_tcp` internals:

1. **Pure protocol core** (`Peer.Handshake`, `FrameBuffer`) — deterministic reducers,
   `step(state, event) → {state, actions}`. ~80% of the logic, 100% offline-unit-tested.
2. **Injectable transport** (`P2P.Transport` behaviour) — `:gen_tcp` in prod, a controllable fake in
   tests. Lets us drive socket events synchronously from a test.
3. **Real loopback integration** — a tiny in-test "fake peer" that genuinely speaks the wire protocol
   over `127.0.0.1`, for one end-to-end confidence test. Plus a `@tag :external` live-mainnet smoke.

## TDD discipline (every task)
RED (write test, run `mix test <file>`, confirm failure reason) → GREEN (minimal) → REFACTOR (+ doc
headers per project rule) → commit `feat(p2p): <task>` (no AI attribution). Async tests where pure;
`async: false` for anything touching a real socket/port.

## Test conventions this phase locks in
- Peer is a `GenServer` started with `%Peer.Config{host, port, network, our_version_fields,
  transport: mod, owner: pid, timeouts: %{...}}`. `transport` defaults to `Athanor.P2P.Transport.Gen`.
- The peer reports lifecycle to `owner` via messages: `{:peer, pid, :ready, peer_version}`,
  `{:peer, pid, :frame, %Frame{}}`, `{:peer, pid, :down, reason}`. Tests assert on these with
  `assert_receive` (no sleeps — satisfies the project no-`Process.sleep` rule).
- Wire timeouts injected tiny in tests (e.g. handshake 50 ms) so timeout paths are fast and deterministic.

---

## T1.0 — Transport behaviour + gen_tcp impl + fake — `Athanor.P2P.Transport`
**RED** — `transport/transport_test.exs`:
- behaviour defines `connect/4`, `send/2`, `setopts/2`, `close/1` (active-mode: data arrives as
  `{:tcp, socket, bin}` / `{:tcp_closed, socket}` / `{:tcp_error, socket, reason}` to the owning process).
- `Transport.Gen` round-trips bytes against a throwaway loopback listener: connect → send → receive the
  `{:tcp, _, bytes}` message echoed by the listener.
- `Transport.Fake` (test support): `connect` returns a handle bound to the calling test; `send/2`
  records outbound bytes retrievable via `Fake.sent(handle)`; `Fake.deliver(handle, bytes)` injects an
  inbound `{:tcp, ...}` message to the peer process. Assert send-capture and deliver-injection work.

**GREEN:** thin wrappers over `:gen_tcp` (`mode: :binary, active: :once, nodelay: true, packet: :raw`);
Fake is a small Agent/struct.
**REFACTOR:** keep the behaviour minimal; Fake lives in `test/support/`.

---

## T1.1 — receive buffer accumulator — `Athanor.P2P.FrameBuffer` (pure)
TCP delivers arbitrary byte chunks; this reassembles them into whole frames.

**RED** — `frame_buffer_test.exs`:
- `new/1` (carries `network` + `max_payload`); `push(buf, bytes) → {frames, buf}`.
- **whole frame in one push:** push a full verack → `{[%Frame{command: "verack"}], empty_buf}`.
- **split frame:** push first 10 bytes → `{[], buf}`; push the rest → `{[frame], buf}`.
- **multiple frames in one push:** concat 3 frames → `{[f1, f2, f3], buf}` in order.
- **frame + partial:** 1 full frame + 10 bytes of the next → `{[f1], buf}` with the 10 bytes retained.
- **error propagation:** a bad-magic chunk → `{:error, :bad_magic}` (or surfaces the Frame.decode error;
  pick one contract and test it — recommended: `push` returns `{:error, reason, buf}` and the Peer
  treats it as fatal).

**GREEN:** loop `Frame.decode/2` over the accumulated binary until `:need_more`, collecting frames.
**REFACTOR:** tail-recursive `drain/2`.

---

## T1.2 — handshake reducer (pure) — `Athanor.P2P.Peer.Handshake`
The heart of Phase 1, fully testable without IO. Mirrors consigliere's inline handshake.

State: `%Handshake{got_peer_version?, got_peer_verack?, sent_our_verack?, peer_version, status}`.
`step(state, event) → {state, actions}` where actions ∈ `{:send, frame_bin}` | `{:done, peer_version}` |
`{:error, reason}`. Events: `:start`, `{:frame, %Frame{}}`, `:timeout`.

**RED** — `peer/handshake_test.exs`:
- `step(new, :start)` → emits `{:send, <version frame>}`, status `:awaiting`.
- receiving peer `version` → emits `[{:send, verack}, {:send, protoconf}]`, sets `got_peer_version?` and
  `sent_our_verack?`, stores parsed `peer_version`.
- receiving peer `verack` → sets `got_peer_verack?`; **when all three flags true → emits `{:done, peer_version}`**.
- **order independence:** verack-before-version reaches `:done` too (drive both orderings; assert same final state).
- `ping` frame mid-handshake → emits `{:send, pong}` with the echoed nonce, does not complete handshake.
- unrelated frames mid-handshake (`sendheaders`, `addr`, `feefilter`) → no actions, no completion (ignored).
- `reject` during handshake → `{:error, :handshake_rejected}`.
- `:timeout` before completion → `{:error, :handshake_timeout}`.
- malformed version payload → `{:error, :bad_version}` (parse failure is fatal).

**GREEN:** a pure `case` over `{event, state}`; no process, no socket.
**REFACTOR:** derive `done?` from the three booleans in one place.

---

## T1.3 — Peer GenServer: connect + drive handshake — `Athanor.P2P.Peer`
Wires T1.0–T1.2 together over the **Fake** transport (deterministic).

**RED** — `peer/peer_test.exs` (uses `Transport.Fake`, `start_supervised!`):
- on start, Peer connects and the Fake captures an outbound **version** frame as the first bytes sent.
- `Fake.deliver` a peer `version` then `verack` → owner receives `{:peer, pid, :ready, peer_version}`;
  Fake shows our `verack` + `protoconf` were sent.
- after `:ready`, Peer issues a `getaddr` (assert it appears in Fake.sent) — seeds discovery for Phase 2.
- handshake timeout: deliver nothing within injected 50 ms → owner gets `{:peer, pid, :down, :handshake_timeout}`
  and the process exits (`Process.monitor` + `assert_receive {:DOWN, ...}`; **no sleeps**).
- connect failure (Fake configured to refuse) → `{:peer, pid, :down, {:connect, reason}}`, process exits.

**GREEN:** `init` → `{:ok, state, {:continue, :connect}}`; `handle_continue` connects, runs
`Handshake.step(:start)`, sends; `handle_info({:tcp,...})` pushes into FrameBuffer, folds frames through
`Handshake.step`, performs `{:send,_}` actions, and on `{:done, v}` transitions to `:ready` + notifies owner;
arm a `handshake_timeout` via `Process.send_after`.
**REFACTOR:** one `apply_actions/2` executes the reducer's action list (shared with T1.4).

---

## T1.4 — steady-state receive loop + dispatch
After `:ready`, frames are forwarded to the owner and protocol housekeeping is handled locally.

**RED** — `peer/peer_steady_test.exs` (Fake transport):
- post-ready, `Fake.deliver` an `inv` frame → owner receives `{:peer, pid, :frame, %Frame{command: "inv"}}`.
- `Fake.deliver` a `ping(nonce)` → Peer auto-replies `pong(nonce)` (assert via Fake.sent); owner is NOT
  bothered with ping/pong (filtered).
- `Fake.deliver` two frames in one chunk → owner receives both, in order (FrameBuffer integration).
- a `{:tcp, _, <bad magic>}` chunk → Peer exits `{:peer, pid, :down, :bad_magic}` (fatal decode error).
- back-pressure sanity: Peer re-arms `active: :once` (via `Transport.setopts`) after each chunk — assert
  `setopts` called (Fake records it) so a fast peer can't flood the mailbox unbounded.

**GREEN:** extend `handle_info({:tcp,...})`: drain buffer, for each frame either handle locally
(ping→pong) or forward to owner; re-arm `:once`.
**REFACTOR:** a `dispatch_frame/2` with a small local-handler table.

---

## T1.5 — keepalive ping + inactivity timeout
**RED** — `peer/peer_keepalive_test.exs` (injected tiny intervals):
- with `ping_interval: 30 ms`, after ready the Peer emits a `ping` (Fake.sent shows it) and tracks the nonce.
- delivering the matching `pong` clears the in-flight ping (no disconnect).
- `inactivity_timeout: 60 ms` with **no** inbound traffic → Peer exits `{:peer, pid, :down, :inactivity_timeout}`.
- any inbound frame resets the inactivity clock (deliver a frame at 40 ms, assert still alive past 60 ms).

**GREEN:** two `Process.send_after` timers; reset inactivity on every inbound chunk; track last ping nonce.
**REFACTOR:** centralize timer (re)arming.

---

## T1.6 — teardown & disconnect semantics
**RED** — `peer/peer_teardown_test.exs`:
- `{:tcp_closed, socket}` → `{:peer, pid, :down, :closed}`, `terminate` calls `Transport.close` (idempotent),
  process exits `:normal`-ish (a restartable reason, not a crash that trips the supervisor's max-restarts spuriously).
- `{:tcp_error, socket, reason}` → `{:peer, pid, :down, {:tcp_error, reason}}`.
- a `reject` frame after ready → forwarded to owner (it's not fatal post-handshake); owner decides.
- explicit `Peer.stop(pid)` → graceful close, `:down, :stopped`.
- **DOWN assertions use `Process.monitor`/`assert_receive {:DOWN,...}`**, never `Process.alive?` polling.

**GREEN:** implement `terminate/2` + the `{:tcp_closed,_}`/`{:tcp_error,_}` clauses; map reasons.
**REFACTOR:** single `disconnect(state, reason)` helper used by all exit paths.

---

## T1.7 — loopback integration: real fake peer — `peer/integration_test.exs` (`async: false`)
One end-to-end test over a genuine `127.0.0.1` socket — no Fake — to prove the `:gen_tcp` wiring,
active-mode messages, and byte framing actually work together.

**RED:**
- start an in-test `FakePeerServer` (a `:gen_tcp` listener) that: accepts, reads our `version`,
  replies `version` + `verack`, then after our `verack` sends an `inv` with one tx hash, then a `ping`.
- start `Peer` pointed at the listener's ephemeral port with `Transport.Gen`.
- assert owner receives `{:peer, _, :ready, v}` with a sane `peer_version`.
- assert owner receives `{:peer, _, :frame, %Frame{command: "inv"}}` carrying the expected hash.
- assert the FakePeerServer received a `pong` answering its `ping`.
- close the listener → owner gets `{:peer, _, :down, :closed}`.

**GREEN:** should pass if T1.0–T1.6 are correct; if not, this exposes a real-socket-only bug
(active:once re-arming, partial reads). Fix minimally.
**REFACTOR:** extract `FakePeerServer` to `test/support/` for reuse in Phase 2 pool tests.

---

## T1.8 — live mainnet smoke (manual / CI-skipped) — `@tag :external`
**RED/GREEN:** `@tag :external` (excluded by default in `test_helper.exs`):
- connect `Peer` to a real seed-resolved mainnet node; `assert_receive {:peer, _, :ready, v}, 10_000`
  with `v.start_height > 800_000` and `v.user_agent =~ "/"`; expect at least one inbound `inv` within a few seconds.
- Documents the one true external dependency check; run with `mix test --only external`.

**GREEN:** none — pure observation. If it fails where T1.7 passed, suspect magic/seed/UA-filtering, not framing.

---

## Design notes the tests enforce (carry into Phase 2)
- **Pure core, thin shell:** all protocol decisions live in `Handshake` + `FrameBuffer` (pure); the
  GenServer only does IO, timers, and owner notification. Phase 2's pool reuses the same Peer untouched.
- **Owner-process indirection:** Peer talks to an `owner` pid, not directly to the indexer. In Phase 2
  the owner is the `PeerPool`/registry; in tests it's `self()`. Keeps the Peer ingest-agnostic.
- **active: :once** everywhere — bounded mailbox, no flooding. Asserted in T1.4.
- **Restartable exits:** disconnects are normal lifecycle, not crashes — Phase 2 supervisor uses a
  transient/temporary restart with backoff; tests confirm reasons are clean atoms/tuples.

## Definition of Done (Phase 1)
- T1.0–T1.7 green; `mix test test/athanor/p2p` clean (T1.8 excluded by default).
- No `Process.sleep`/`Process.alive?` in tests — only `assert_receive` + `Process.monitor` DOWN.
- Handshake logic provably correct in both frame orderings and all four failure modes
  (timeout, reject, bad version, connect-fail) via the pure reducer.
- One real-socket loopback test proves end-to-end framing; one tagged live test proves wire-correctness
  against mainnet.
- Peer is fully transport-injectable (no hard `:gen_tcp` dependency in the GenServer logic).

## Suggested commit sequence
`T1.0 transport → T1.1 frame_buffer → T1.2 handshake_reducer → T1.3 peer_connect →
T1.4 steady_loop → T1.5 keepalive → T1.6 teardown → T1.7 loopback_integration → T1.8 live_smoke`.
Highest-risk: **T1.2** (handshake correctness — most cases) and **T1.7** (real-socket reality check).
```
