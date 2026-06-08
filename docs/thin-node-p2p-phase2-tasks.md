# Phase 2 — Peer Pool + Discovery: TDD Task Breakdown

Companion to `thin-node-p2p-plan.md`, `thin-node-p2p-phase0-tasks.md`, and `thin-node-p2p-phase1-tasks.md`.
Scope: maintain a **pool of N healthy outbound peers** — bootstrap from **DNS seeds + `addr` gossip**
(hardcoded fallback seeds are explicitly **deferred** this phase; see T2.2), enforce **/24 subnet
diversity**, apply a **negative cooldown** on failures, and
**self-heal** back to N when peers drop. Plus the §10.1 supervision-placement contract so the pool can
be wired into the app (config-gated, default off).

Depends on Phase 1 (`Peer`, `Peer.Config`, `Transport`, owner-message protocol) being green. No ingest,
no broadcast, no headers (Phase 3+). The pool is the `owner` of every `Peer` it starts; it does not yet
*do* anything with the frames a peer forwards (that's Phase 3) — in this phase frames are counted/ignored.

## Core TDD strategy (same shape as Phase 1)
Split the logic so almost everything is a pure, offline-tested reducer and the GenServer is a thin shell:

1. **Pure address book** (`PeerPool.AddrBook`) — a deterministic reducer over known/candidate addresses
   that, given the current live set, cooldown clock, and target N, decides **which address to dial next**
   while preserving /24 diversity. Time is injected (`now_ms` passed in), so no wall-clock flakiness.
2. **Injectable peer starter** — the pool starts peers through a `peer_starter` function/module (default
   `Athanor.P2P.Peer`), so pool tests use a controllable fake that never opens a socket.
3. **Injectable resolver** — `Discovery` resolves DNS seeds through an injected resolver fun (default
   `:inet.getaddrs/2`), so seed-bootstrap logic is tested deterministically; the live resolve is the
   `@tag :external` smoke (T2.7).

## TDD discipline (every task)
RED (write test, run `mix test <file>`, confirm the failure reason) → GREEN (minimal) → REFACTOR (+ doc
headers per project rule) → commit `feat(p2p): <task>` (no AI attribution). `async: true` for pure
modules; `async: false` for anything starting real processes/sockets. **No `Process.sleep` / `Process.alive?`
in tests** — use `assert_receive` + `Process.monitor` DOWN, and inject time/timers as in Phase 1 (T1.5).

## Conventions this phase locks in
- An **address** is `{ip :: :inet.ip4_address(), port :: :inet.port_number()}` internally; the wire/string
  forms convert only at the edges. `/24` key = the first three octets of an IPv4 address.
- **Canonical `%PeerPool.Config{}`** (single source of truth; T2.3 uses exactly this):
  `network, target, our_version, peer_starter, resolver, transport, transport_opts, seeds, cooldown_ms,
  now_fun`. The pool is the `owner` passed in each child `Peer.Config` (so it receives
  `{:peer, pid, :ready|:frame|:down, _}`) and forwards `transport`/`transport_opts` into each child.
  - `seeds` (default `[]`) is an **explicit `[{ip, port}]` override** seeded into the `AddrBook` at init
    (for deterministic tests / static/known peers). It is **unioned with** DNS + fallback discovery, never
    a replacement — discovery still runs. Tests set `seeds` + a stub `resolver` for determinism; prod
    leaves `seeds: []` and relies on discovery.
- The pool keeps a `PeerRegistry` of live peers keyed by `{ip, port}`; `:ready` promotes a pending dial to
  live, `:down` removes it and starts a cooldown.
- Diversity invariant (asserted repeatedly): **no two live peers share a /24**.

---

## T2.0 — Address book reducer (pure) — `Athanor.P2P.PeerPool.AddrBook`
The brain of the pool, fully testable with no process. Tracks candidate addresses, live addresses,
and per-address cooldown deadlines, and answers "what should I dial next?".

State carries an explicit **in-flight (`dialing`) set** so the contract for un-resolved dials is testable
and the pool can never double-dial an address (or a same-/24 neighbour) while a dial is pending:
`%AddrBook{candidates :: MapSet, dialing :: MapSet, live :: %{addr => meta}, cooldown :: %{addr => until_ms},
target :: pos_integer}`.

A "slot" counts both live and dialing peers: `used = map_size(live) + MapSet.size(dialing)`.

**RED** — `peer_pool/addr_book_test.exs`:
- `new/1` (carries `target`); `add_candidates/2` unions new addresses (dedup).
- `dial_targets(book, now_ms)` returns up to `target - used` addresses to dial, and:
  - **excludes** addresses already live **or in `dialing`**.
  - **excludes** any address whose /24 is occupied by a live **or dialing** peer.
  - **excludes** addresses whose cooldown deadline is `> now_ms`; **includes** them once expired.
  - returns `[]` when `used >= target`.
- **dial lifecycle (the in-flight contract):**
  - `mark_dialing(book, addr)` moves `candidate → dialing` (reserves its /24 immediately).
  - `promote(book, addr)` moves `dialing → live` (on `{:peer, _, :ready, _}`).
  - `fail_dial(book, addr, now_ms, cooldown_ms)` moves `dialing → cooldown` (dial errored / never reached
    ready) and sets deadline `now_ms + cooldown_ms`.
  - `release(book, addr, now_ms, cooldown_ms)` moves `live → cooldown` (a ready peer dropped).
  - assert: a `mark_dialing`'d address (and its /24) is excluded from the next `dial_targets`; after
    `fail_dial` it is not re-dialed until its cooldown expires; after `promote` it is live and its /24
    stays reserved.
- diversity helper `occupied_slash24s/1` (live ∪ dialing) and a **property**: after any interleaving of
  `mark_dialing`/`promote`/`fail_dial`/`release`, no two live-or-dialing addresses share a /24.

**GREEN:** plain maps/MapSets + a pure `case`/`Enum` selection; `/24` = `{a, b, c}` from `{a,b,c,d}`.
**REFACTOR:** one `eligible?/3` predicate shared by `dial_targets`; one `occupied_slash24s/1` over live∪dialing.

---

## T2.1 — Peer registry — `Athanor.P2P.PeerRegistry`
A view of live peers keyed by `{ip, port}` with reverse pid lookup. **Cleanup decision (locked):** a bare
`Registry.register(addr, pid)` would be **wrong here** — Registry entries are owned by the *calling*
process, so an entry keyed by `{ip, port}` and registered by the pool would survive the *peer's* death and
make `addresses/0`/`slash24s/0` lie about pool health. Two correct options:

  - **(A) self-registration:** each `Peer` registers *itself* under its `{ip, port}` via a
    `{Registry, keys: :unique, name: ...}` and a `:via` tuple, so the entry is owned by the peer process
    and auto-removed when it dies. Requires threading the registry name + address into `Peer.Config`.
  - **(B, chosen) monitor-backed wrapper:** `PeerRegistry` is a small `GenServer` holding a map
    `{ip,port} => pid` (+ reverse `pid => {ip,port}`). `register/2` stores the pair **and
    `Process.monitor(pid)`**; on `{:DOWN, _, :process, pid, _}` it removes that peer. This keeps `Peer`
    ignorant of the registry (preserves the Phase 1 owner-indirection seam) and centralises diversity
    queries. We choose (B).

**RED** — `peer_registry_test.exs` (chosen wrapper):
- `child_spec/1` / `start_link/1` start the registry `GenServer` under `start_supervised!`.
- `register(addr, pid)` / `lookup(addr)` / `unregister(addr)` round-trip; `addresses/0` lists live
  `{ip, port}`; `slash24s/0` returns their /24 set.
- **cleanup on real death:** register a throwaway process, `Process.monitor` it, kill it, `assert_receive`
  its `{:DOWN, …}`, then assert `lookup(addr) == :error` and `addresses/0` no longer lists it — proving the
  monitor-driven removal fires when the *actual peer* dies (no sleep, no `Process.alive?`).
- a second `register` for an already-taken `{ip, port}` is rejected (`:unique`).

**GREEN:** a ~40-line `GenServer`: `handle_call` for register/unregister/lookup, `handle_info` for `:DOWN`,
two maps for forward + reverse lookup.
**REFACTOR:** derive `slash24s/0` from `addresses/0`; expose only what the pool needs.

---

## T2.2 — Discovery (seed bootstrap + addr absorb) — `Athanor.P2P.Discovery`
Turns seeds and gossip into candidate addresses; the only IO (DNS) is injected.

**RED** — `discovery_test.exs`:
- `seed_candidates(network, resolver)` resolves each DNS seed via the injected `resolver` fun
  (`fn host -> {:ok, [ip4]} | {:error, _} end`), unions results with the network's **fallback seeds**,
  pairs each IP with the network default port, and dedups. A failing resolver for one seed doesn't sink
  the others.
- `fallback_seeds(network)` returns the network's hardcoded fallback IP list. **Decision (locked):
  fallback seeds are explicitly DEFERRED in Phase 2** — the plan (§4, revised per MR !2 review) notes
  hardcoded `pnSeed6`-style IP tables go stale quickly, so the **required** bootstrap contract for this
  phase is **DNS seeds + `addr` gossip** (both self-refreshing). `fallback_seeds/1` may therefore return
  `[]`; the test asserts the function exists and that `seed_candidates/2` works **with DNS alone** (no
  fallback dependency). Populating real fallback IPs is a later hardening task, not a Phase-2 gate.
  Accordingly the Phase-2 scope and DoD below do **not** require non-empty fallback seeds.
- `absorb_addr(addr_entries)` maps decoded `addr` gossip entries (from `Messages.Addr`) to
  `{ip, port}` candidates, dropping non-IPv4 / unroutable (0.0.0.0, RFC1918) addresses.

**GREEN:** thin functions over the injected resolver + `Network` fields + `Messages.Addr` output.
**REFACTOR:** centralize the "routable IPv4?" guard (reused by AddrBook ingestion).

---

## T2.3 — PeerPool GenServer: maintain target N — `Athanor.P2P.PeerPool`
Wires T2.0–T2.2 together over a **fake peer starter** (deterministic, no sockets).

Uses the **canonical `%PeerPool.Config{}`** defined in Conventions above (`network, target, our_version,
peer_starter, resolver, transport, transport_opts, seeds, cooldown_ms, now_fun`) — `peer_starter` defaults
to `&Athanor.P2P.Peer.start_link/1`; tests inject a fake that returns a controllable pid and records the
`Peer.Config` it was handed, set `seeds` + a stub `resolver` for determinism, and drive `now_fun`.

**RED** — `peer_pool/peer_pool_test.exs` (fake starter, `start_supervised!`):
- on start, the pool resolves seeds and **dials up to `target`** peers — assert the fake starter was
  invoked `target` times with distinct /24 addresses, each `Peer.Config.owner == pool`.
- a child `{:peer, pid, :ready, v}` promotes that dial to live (registry shows it; `AddrBook.live` grows).
- a child `{:peer, pid, :down, reason}` removes it, sets a cooldown, and the pool **re-dials** to refill
  to `target` (self-heal) — assert a replacement dial to a fresh /24, and that the cooled-down address is
  not redialed until its deadline (drive `now_fun`).
- diversity: the pool never dials an address whose /24 is already live or pending.
- when candidates are exhausted below target, the pool stays at what it has and re-attempts after a
  refresh tick (inject the tick message; no sleep).

**GREEN:** `init` → resolve seeds into `AddrBook`, `{:continue, :fill}`; `fill` dials `AddrBook.dial_targets`;
`handle_info({:peer, …})` updates registry + book and re-fills; cooldown via injected `now_fun`.
**REFACTOR:** one `fill/1` used by continue, `:down`, and the refresh tick.

---

## T2.4 — Absorb `addr` gossip into the pool
Live peers forward `addr` frames (Phase 1 forwards all non-handshake frames to the owner = pool).

**RED** — `peer_pool/peer_pool_gossip_test.exs`:
- delivering `{:peer, pid, :frame, %Frame{command: "addr", payload: …}}` to the pool parses it via
  `Messages.Addr`, runs `Discovery.absorb_addr`, and unions routable candidates into the `AddrBook`
  (assert via a subsequent fill dialing a gossiped address).
- non-routable / duplicate gossip is ignored (book unchanged).
- a non-`addr` frame forwarded to the pool is ignored in this phase (counted, not acted on).

**GREEN:** add an `addr` clause to the pool's `{:peer, _, :frame, _}` handler → absorb → maybe-fill.
**REFACTOR:** share the routable-IPv4 guard from T2.2.

---

## T2.5 — Supervision placement + config gating (§10.1) — `Athanor.P2P.Supervisor`
Make the pool wireable into the app, **off by default**, with explicit restart semantics.

**RED** — `p2p/supervisor_test.exs` + `application_test.exs` (or a focused config test):
- `Athanor.P2P.Supervisor` starts `PeerRegistry` then `PeerPool` (`:rest_for_one`: registry restart
  takes the pool with it, not vice-versa) — assert child order + strategy.
- config gate: with `config :athanor, Athanor.P2P, enabled: false` (the default), the app tree does **not**
  include the P2P supervisor; with `enabled: true` it does. Assert via the running supervision tree.
- restart behavior: killing `PeerPool` restarts it (and re-bootstraps); killing `PeerRegistry` restarts
  both, in order. Use `Process.monitor` + `assert_receive {:DOWN, …}` then assert the tree recovered
  (`:sys.get_state`/`Supervisor.which_children`) — no sleeps.

**GREEN:** a `Supervisor` with the two children; an `enabled?/0` config read; conditionally add the child
in the app supervisor (decide child-of-`Blockchain.Supervisor` vs sibling here — default sibling, gated).
**REFACTOR:** one `enabled?/0`; document the placement decision in a moduledoc.

---

## T2.6 — Integration: pool over real sockets — `peer_pool/integration_test.exs` (`async: false`)
Prove the pool drives real `Peer`s end to end, self-heals, and keeps diversity — using several
`FakePeerServer`s (from Phase 1's `test/support`) on distinct loopback ports.

**Diversity strategy (locked):** **keep diversity enabled** and give each fake peer a **distinct synthetic
/24** while the socket still connects to a loopback port. Concretely, the AddrBook/registry sees the
peer's *advertised* address `{10, 0, n, 1}` (distinct /24 per n), but the `Transport`'s `connect/4`
receives a `transport_opts` **address-rewrite map** `%{ {10,0,n,1} => {{127,0,0,1}, loopback_port_n} }`
so the real dial lands on the `FakePeerServer`. (`Transport.Gen` gains a tiny test-only rewrite hook, or
the pool maps address→connect-target via config.) This keeps the integration test proving the **real**
/24-diversity invariant under real process churn — the option of "relax diversity" is explicitly rejected
because it would stop the one real-socket test from protecting the invariant it exists to protect.

**RED:**
- start K `FakePeerServer`s on distinct loopback ports; seed the pool (via `seeds`/stub `resolver`) with K
  **distinct-/24 synthetic** addresses, each rewritten to a loopback port as above.
- assert the pool reaches `target` live peers (`PeerRegistry.addresses/0`) and `PeerRegistry.slash24s/0`
  has `target` distinct /24s (diversity holds with real sockets).
- kill one `FakePeerServer`; assert the pool observes `:down`, redials a **fresh** synthetic /24, and
  returns to `target` (self-heal) — `Process.monitor` the replaced peer; `assert_receive` the new `:ready`.

**GREEN:** should pass if T2.0–T2.5 are correct; fix any real-process-only races minimally.
**REFACTOR:** extract a `start_n_fake_servers/1` helper to `test/support`.

---

## T2.7 — Live smoke (manual / CI-skipped) — `@tag :external`
**RED/GREEN:** `@tag :external` (excluded by default per Phase 1 T1.S):
- start a real `PeerPool` on **testnet** (default) with the real resolver; assert it reaches at least
  `min(target, 3)` live peers within ~30 s and that `PeerRegistry.slash24s/0` has no duplicates.
- mainnet variant opt-in via `P2P_SMOKE_NETWORK=mainnet`.
- run with `mix test --only external`.

**GREEN:** none — observation only. If it fails where T2.6 passed, suspect seed resolution / UA-filtering
/ diversity starving the pool on a thin network, not pool logic.

---

## Design notes the tests enforce (carry into Phase 3)
- **Pure brain, thin shell:** all dial/diversity/cooldown decisions live in `AddrBook` (pure); the
  GenServer only does process lifecycle, registry updates, and timers. Phase 3's ingest attaches to the
  same pool without touching this logic.
- **Pool is the peer owner:** `{:peer, _, :frame, _}` flows to the pool; in Phase 3 the pool (or a
  delegated `MempoolObserver`) consumes `inv`/`tx`. Keep the seam clean.
- **Injected time + starter + resolver:** zero wall-clock dependence in unit tests; the only real-network
  test is the tagged smoke.
- **Diversity + cooldown are invariants, not best-effort:** asserted by property tests and the
  self-heal integration test.

## Definition of Done (Phase 2)
- T2.0–T2.6 green; `mix test test/athanor/p2p` clean (T2.7 excluded by default).
- No `Process.sleep`/`Process.alive?` in tests; time/timers injected.
- Pool provably: bootstraps to N from **DNS + gossip** (no fallback-seed dependency), self-heals to N
  after a drop, never violates /24 diversity (including in-flight dials), honors negative cooldown, and
  absorbs `addr` gossip.
- `AddrBook` tracks in-flight dials (`dialing`) so an address/its /24 is reserved from `mark_dialing`
  until `promote`/`fail_dial`.
- `PeerRegistry` removes a peer when its **actual process dies** (monitor-driven), proven by a test that
  kills the registered process.
- P2P supervisor is config-gated (default off) with tested restart semantics and a documented placement.
- One real-socket integration test (diversity enabled via synthetic /24s) + one tagged live smoke.

## Suggested commit sequence
`T2.0 addr_book → T2.1 registry → T2.2 discovery → T2.3 peer_pool → T2.4 gossip → T2.5 supervisor →
T2.6 integration → T2.7 live_smoke`.
Highest-risk: **T2.3** (pool self-heal + diversity under churn) and **T2.6** (real-process reality check).
