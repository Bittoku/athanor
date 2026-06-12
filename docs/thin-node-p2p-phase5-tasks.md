# Phase 5 — Capability router + make P2P primary (TDD task breakdown)

Phases 0–4 built the native P2P stack bottom-up: wire codec, single peer + handshake, peer pool +
discovery, the **inbound** mempool observer (`inv → getdata → tx → verify → index`), and the **outbound**
broadcast + relay-back path. Phase 5 ties it into the existing provider call sites: introduce a **capability
router** so "which source serves X" is *config, not hardcoded*, add a **P2P pull-fetch for raw txs** (a
`getdata` with a short timeout, mempool-only), wire it into the back-to-genesis parent-fetch, and route the
already-P2P-primary broadcast through the same router — all while preserving **cold-start safety** (the DXS
rule: P2P-primary with zero peers must still work via REST/RPC fallback, byte-for-byte as today).

This doc specifies the contracts that must be settled **before** implementation (per the plan's
"specify-before-the-phase" rule, §10.3), then the bottom-up TDD tasks. Same shape as Phases 1–4: pure
reducers decide, thin GenServers do IO; inject time/timers/transport/peers; no `Process.sleep`/
`Process.alive?`; commit `feat(p2p): <task>` (no AI attribution); format under the project's Elixir **1.15**
toolchain (import_deps-aware — `field`/query macros stay paren-less).

---

## §0 — Direct-call inventory (settle FIRST, per plan §10.3)

Routing is introduced against a **known call-site map**, not ad hoc. Every application call to an external
provider, grouped by the capability it serves:

| Capability | Call sites (today) | Provider(s) |
|---|---|---|
| **`broadcast`** | `services/broadcast.ex` (Phase 4 `broadcast_tx/2`) | **already routed**: P2P relay primary, RPC `:broadcaster` fallback |
| **`raw_tx_fetch`** | `indexer/b2g_resolver.ex:115/120/126` `fetch_remote/1` cascade; `block_processor.ex:112`; `missing_tx_syncer.ex:67`; `unconfirmed_monitor.ex:56` | RPC `getrawtransaction` → JungleBus → WhatsOnChain (hardcoded order in b2g) |
| **`realtime_ingest`** | `zmq_listener.ex` (rawtx/hashblock); `jungle_bus_client.ex` SSE `stream_sse/3`; P2P `MempoolObserver` (Phase 3) | **parallel fan-in** — all publish into `TransactionProcessor`, dedup downstream (Phase 3 §A source tags) |
| **`validation_fetch`** | `unconfirmed_monitor.ex:56` (verbose tx); `chain_tip_verifier.ex:45-46`; `sync_status.ex:40` | RPC (`getrawtransaction`/`getblockcount`/`getblockhash`) |
| **`block_backfill`** | `block_processor.ex:77`; `chain_tip_verifier.ex:85`; `jungle_bus_client.ex` poll | RPC `getblock`/`getblockhash`; JungleBus |
| **`historical_scan`** | `missing_tx_syncer.ex:47` | WhatsOnChain `address/history` |
| **`balance_utxo_fetch`** | `infra/whats_on_chain.ex` `get_address_utxos`; `infra/bitails.ex` (defined, unused) | WhatsOnChain / Bitails REST |

**Phase 5 touches exactly two of these**, leaving the rest REST/RPC (honest — P2P cannot serve them):
1. **`raw_tx_fetch`** — prepend a P2P `getdata` fetch to the b2g cascade (mempool hits skip REST). The §B work.
2. **`broadcast`** — express Phase 4's existing P2P-primary routing *through the router* (no behavior change). The §C work.

`validation_fetch` / `block_backfill` / `historical_scan` / `balance_utxo_fetch` **stay REST/RPC** — a peer
cannot authoritatively answer "is this UTXO unspent", serve an arbitrary historical/confirmed tx, or a Merkle
proof. Claiming P2P primary there would be dishonest and break correctness. `realtime_ingest` stays a
**fan-in** (see §C) — *not* an exclusive cascade.

---

## §A — Capability router (`Athanor.P2P.SourceRouter`)

A **pure, config-driven** resolver — no process, no IO. It answers *one* question: for a capability, what is
the ordered provider preference?

```
resolve(capability) :: {primary :: provider, fallbacks :: [provider]}
```

- **Capabilities:** `:raw_tx_fetch | :broadcast | :realtime_ingest | :validation_fetch | :block_backfill |
  :historical_scan | :balance_utxo_fetch`.
- **Providers (tags):** `:p2p | :rpc | :whatsonchain | :bitails | :junglebus | :zmq`.
- **Config:** `config :athanor, Athanor.P2P.SourceRouter, routes: %{capability => {primary, [fallbacks]}}`,
  deep-merged over the **honest defaults** below. An unknown capability raises (programmer error, not runtime).

**Default route table (the honesty contract):**

| capability | primary | fallbacks | rationale |
|---|---|---|---|
| `raw_tx_fetch` | `:p2p` | `[:rpc, :junglebus, :whatsonchain]` | P2P mempool hit skips REST; else today's cascade |
| `broadcast` | `:p2p` | `[:rpc]` | Phase 4 behavior, now expressed here |
| `realtime_ingest` | `:p2p` | `[:zmq, :junglebus]` | **fan-in, not exclusive** — see §C; list = enabled sources, preference for provenance only |
| `validation_fetch` | `:rpc` | `[]` | only a trusted node is authoritative |
| `block_backfill` | `:rpc` | `[:junglebus]` | unchanged |
| `historical_scan` | `:whatsonchain` | `[]` | unchanged |
| `balance_utxo_fetch` | `:whatsonchain` | `[:bitails]` | unchanged |

- The router is **pure resolution only** — it does *not* attempt calls, hold state, or know whether peers
  exist. The *caller* tries `primary` then each `fallback` in order (a thin `route/2` runner helper, §C).
- **`p2p_available?/0` is the caller's gate, not the router's:** when a resolved provider is `:p2p` and
  there are zero live peers (or P2P disabled), the runner treats the P2P attempt as an instant **miss** and
  proceeds to fallbacks — so a `:p2p` primary never blocks cold start. `held = min(2, N−1)`-style peer math
  lives in the P2P providers, never in the router.

**Tests (T5.0).** `resolve/1` returns the default tuple per capability; a config override deep-merges (only
the overridden capability changes); an unknown capability raises; the default table matches the §A contract
exactly (a table-driven test so the honesty defaults can't silently drift).

---

## §B — P2P raw-tx pull-fetch (`raw_tx_fetch`) + b2g wiring

The one genuinely new capability: fetch a **specific** tx by id from the peer set via `getdata`, bounded by a
short timeout. Unlike the Phase-3 observer (which fetches txs a peer *announced* to us), this is a **pull** —
we ask for a txid nobody advertised.

**Honest scope (blocker — don't over-claim).** A BSV node answers `getdata(MSG_TX)` only for txs **in its
mempool**; confirmed txs are typically `notfound`. So P2P `raw_tx_fetch` is a **mempool-only fast path**: a
hit avoids a REST round-trip for an unconfirmed parent (the common b2g case during live ingest); a miss
(`notfound` from all asked peers, or timeout) falls through to the REST/RPC cascade exactly as today. The plan
must state this so we never imply P2P can serve arbitrary historical txs.

**Lifecycle contract (pure `TxFetcher.Tracker`, mirrors the Phase-3 observer reducer).** One in-flight
request per txid:
- `request(txid, peers, now)` → send `getdata(MSG_TX, txid)` to the chosen peers (up to `:fanout`, default
  3, distinct live peers), record `asked = MapSet`, `first_at_ms`.
- `tx(txid, raw, peer)` → **forgery guard**: only if `peer ∈ asked` **and** the payload re-hashes to `txid`
  (wire order) → resolve `{:ok, raw}`; a non-matching payload or unsolicited peer is ignored.
- `notfound(txid, peer)` → drop `peer` from `asked`; when `asked` becomes empty → resolve `:miss`.
- `:tick` / per-request timeout (`:timeout_ms`, default **3_000**) → resolve `:miss` and drop the entry.
- Idempotent resolve: the first of `{:ok,_}` / `:miss` wins; later frames for a resolved txid are ignored.

**`TxFetcher` GenServer (thin shell, frame_sink member).** Folds `tx`/`notfound` frames through the Tracker;
performs `getdata` via `Peer.send_frame`; a synchronous `fetch(txid, opts) :: {:ok, raw_bin} | :miss` is a
`GenServer.call` that rechecks `PeerRegistry.pids` (zero peers → immediate `:miss`, **before** any getdata —
the cold-start gate), picks up to `:fanout` peers via an injected `:selector`, arms the timeout timer, and
replies when the Tracker resolves (reply held via `GenServer.reply` from the resolving frame/timeout). Injected
`:now_fun`/timers/`:selector`/`:fanout`/`:timeout_ms`. It is a **third `frame_sink` consumer** (§A fan-out
already supports a list: `[MempoolObserver, TxRelay, TxFetcher]`) — it only acts on `tx`/`notfound` for txids
it is actively fetching, ignoring everything else (disjoint from the observer's announce-driven fetch).

> Open contract point for Hermes: `tx`/`notfound` are now seen by **two** consumers (observer + fetcher).
> Both keyed on txid; disjoint sets (observer tracks *announced* txids, fetcher tracks *requested* ones). A
> `notfound` for a txid neither tracks is dropped by both. No coordination needed — documented here so the
> double-consumer is a deliberate decision, not an accident.

**b2g wiring (the headline integration).** `b2g_resolver.ex:fetch_remote/1` today hardcodes RPC → JungleBus →
WhatsOnChain. Phase 5 makes it **router-driven**: resolve `:raw_tx_fetch` → run providers in order; the `:p2p`
provider is `TxFetcher.fetch/2` (mempool fast path), the REST/RPC providers are the existing clients. A P2P
hit returns immediately (no REST); a P2P miss continues the cascade unchanged. **Cold-start:** zero peers →
`TxFetcher.fetch` returns `:miss` instantly → identical to today's RPC-first cascade.

**Tests (T5.1/T5.2/T5.3).**
- Reducer (T5.1): request → `getdata` to the asked peers; `tx` with matching hash from an asked peer →
  `{:ok, raw}`; wrong-hash payload ignored (forgery guard); `tx` from an unasked peer ignored; all-`notfound`
  → `:miss`; timeout `:tick` → `:miss`; resolve-once (post-resolve frames ignored).
- GenServer (T5.2, real Peers over `Transport.Fake`): `fetch` sends `getdata` to selector-chosen peers and
  returns the served raw bytes; timeout → `:miss`; `notfound` from all → `:miss`; **zero peers → `:miss` with
  no getdata** (cold-start gate); malformed inbound frame dropped by `Inv`/parse guard before the reducer.
- b2g integration (T5.3): a parent in a peer's mempool is fetched via P2P, **REST clients are not called**
  (asserted via injected flunking REST stubs); on P2P miss the existing RPC→JungleBus→WhatsOnChain cascade
  runs in order; zero peers → cascade runs immediately (cold-start parity).

---

## §C — Make P2P primary through the router + cold-start safety

A thin **route runner** turns a resolved `{primary, fallbacks}` into an attempt sequence, and the two touched
call sites consult it.

**`SourceRouter.route(capability, attempt_fun)` runner.** `attempt_fun.(provider) :: {:ok, result} | :miss |
{:error, reason}`. The runner tries `primary`, then each `fallback`, returning the first `{:ok, _}`; a `:miss`
or `{:error, _}` advances to the next provider; if all miss/error it returns the **last** error (or `:miss` if
all missed). `:p2p` attempts where `p2p_available?/0` is false are skipped as instant `:miss` (cold-start).
Pure except for `p2p_available?` (injected: default `Supervisor.enabled?/0 and PeerRegistry.pids/1 != []`).

**Broadcast (T5.4).** `broadcast_tx/2` currently inlines `peers_available? → relay else RPC`. Re-express it
as `SourceRouter.route(:broadcast, …)` where the `:p2p` attempt is the relay enqueue and `:rpc` is the
`:broadcaster` — **behavior identical** to Phase 4 (the existing T4.2 broadcast tests must stay green
unchanged, including cold-start, saturated/no-peers fallback, the status lattice, and the audit bridge). This
is a *refactor to the router seam*, not a behavior change — the lattice, `apply_relay_event/1`, and arity-1
back-compat are untouched.

**Realtime ingest is a fan-in, NOT an exclusive cascade (blocker — honesty + cold-start).** The inventory
shows `realtime_ingest` is served by ZMQ **and** JungleBus SSE **and** the P2P observer *simultaneously*, all
publishing into `TransactionProcessor` with Phase-3 dedup. Routing it as exclusive primary/fallback would
**drop** ZMQ/SSE whenever P2P has peers — the opposite of cold-start safety. So:
- The router's `:realtime_ingest` entry is **descriptive provenance/preference only**; it does **not** gate
  which realtime sources run. All *enabled* sources keep publishing; the Phase-3 source-tag dedup keeps it to
  one indexed row.
- Phase 5 adds **no** routing switch that can silence a realtime source. (A future phase may add per-source
  enable/disable config; out of scope here.) T5.4 asserts that enabling P2P realtime does **not** stop ZMQ/SSE
  ingest (a regression guard).

**Cold-start safety (the headline verify, T5.5/T5.6).** With P2P configured primary **and zero peers
connected**: `raw_tx_fetch` (b2g parent fetch) and `broadcast` still succeed via REST/RPC fallback,
byte-for-byte as before Phase 5; realtime still flows via ZMQ/SSE. This is the DXS named rule and the gate on
the whole phase.

---

## Tasks (bottom-up, each independently verifiable)

### T5.S — `frame_sink` already a fan-out list (§A confirm) — do FIRST
Phase 4 already widened `frame_sink` to `pid | atom | [..] | nil`. **RED:** a pool with `frame_sink:
[MempoolObserver, TxRelay, TxFetcher]` forwards a post-handshake frame to all three. **GREEN:** none needed if
T4.S holds; **wire `TxFetcher` into `P2P.Supervisor`** as the fourth child (Registry → Observer → TxRelay →
**TxFetcher** → Pool, `rest_for_one`; `frame_sink: [MempoolObserver, TxRelay, TxFetcher]`). **REFACTOR:** keep
single-sink/`nil` fast paths.

### T5.0 — Pure capability router — `Athanor.P2P.SourceRouter` (§A)
**RED:** `source_router_test.exs` — `resolve/1` per-capability default tuples, config deep-merge override,
unknown-capability raise, the full default table (honesty guard). **GREEN:** the resolver over a default map +
`Application.get_env` deep-merge. **REFACTOR:** doc the default table as the single source of truth.

### T5.1 — Pure fetch reducer — `Athanor.P2P.TxFetcher.Tracker` (§B)
**RED:** request → getdata actions; matching `tx` from asked peer → `{:resolve, txid, {:ok, raw}}`; forgery
guard; unsolicited-peer ignore; all-notfound → `:miss`; timeout → `:miss`; resolve-once. **GREEN:** the
reducer (asked set, first-at, idempotent resolve). **REFACTOR:** share the txid wire-order convention + the
`apply_action` shape with Phases 3–4.

### T5.2 — `TxFetcher` GenServer — `Athanor.P2P.TxFetcher` (§B)
**RED:** `tx_fetcher_test.exs` (real `:ready` Peers over `Transport.Fake`): `fetch/2` getdatas the
selector-chosen peers and returns the exact served bytes; timeout → `:miss`; all-notfound → `:miss`; **zero
peers → `:miss`, no getdata**; malformed frame dropped before the reducer; concurrent `fetch` for distinct
txids don't cross-resolve. **GREEN/REFACTOR:** thin shell; held `GenServer.reply`; injected
selector/fanout/timeout/now_fun.

### T5.3 — b2g parent-fetch through the router — `indexer/b2g_resolver.ex` (§B)
**RED:** `b2g_resolver` additions — a mempool parent is fetched via P2P with the REST/RPC clients **not**
called (flunking stubs); a P2P miss falls through to RPC → JungleBus → WhatsOnChain **in order**; zero peers →
cascade runs immediately (cold-start parity, output identical to today). **GREEN:** route `:raw_tx_fetch`
through `SourceRouter.route`, P2P provider = `TxFetcher.fetch`, others = existing clients; inject the providers
as seams. **REFACTOR:** keep `fetch_remote/1`'s public result shape unchanged.

### T5.4 — Broadcast + realtime through the router — `services/broadcast.ex` + supervisor wiring (§C)
**RED:** the **existing** T4.2 broadcast tests stay green after re-expressing routing via
`SourceRouter.route(:broadcast, …)` (cold-start, saturated/no-peers → RPC, lattice, audit bridge, arity-1 all
unchanged); a new test asserts enabling P2P realtime does **not** stop ZMQ/SSE publishing (fan-in regression
guard). **GREEN:** the `route/2` runner + `p2p_available?` gate; swap broadcast's inline check for the runner.
**REFACTOR:** one routing authority (`SourceRouter`), no second broadcast API.

### T5.5 — Integration: b2g P2P fast path over a real socket — `tx_fetcher/integration_test.exs` (`async: false`)
End-to-end through the real `P2P.Supervisor` + a `FakePeerServer` holding a tx in "mempool" (answers our
unsolicited `getdata` with the `tx`). A b2g parent resolve for that txid returns via P2P with **no** REST call;
a resolve for an absent txid gets `notfound` → REST fallback. **Cold-start case:** with zero peers, the same
resolve immediately uses REST (asserts no regression).

### T5.6 — Live smoke (`@tag :external`, testnet, CI-skipped) — `tx_fetcher/live_smoke_test.exs`
Against live peers: bootstrap ≥1 peer, then `TxFetcher.fetch/2` a txid known to be in the testnet mempool
(operator-supplied via `P2P_SMOKE_FETCH_TXID`) and assert `{:ok, raw}` whose hash matches; with no txid
supplied, assert a random/absent txid returns `:miss` within the timeout (proving the getdata→notfound/timeout
path). `mix test --only external`; mainnet via `P2P_SMOKE_NETWORK=mainnet`.

---

## Definition of Done (Phase 5)
- T5.S, T5.0–T5.5 green; `mix test test/athanor/p2p` + the b2g/broadcast tests clean (T5.6 excluded by default).
- No `Process.sleep`/`Process.alive?`; `now_fun`/timers/peers/providers injected.
- **Capability router** (`SourceRouter`) is the single config authority: `resolve/1` returns `{primary,
  fallbacks}` from honest defaults (table-tested); `route/2` tries primary→fallbacks, skipping a `:p2p` primary
  as instant miss when `p2p_available?` is false.
- **Honest routing:** only `raw_tx_fetch` and `broadcast` are P2P-primary; `validation_fetch`/`block_backfill`/
  `historical_scan`/`balance_utxo_fetch` stay REST/RPC; `realtime_ingest` is a **fan-in** the router never
  gates (enabling P2P realtime does not silence ZMQ/SSE — asserted).
- **P2P raw-tx pull-fetch** is mempool-only and honest: `TxFetcher.fetch/2` getdatas up to `:fanout` peers,
  returns `{:ok, raw}` on a hash-verified hit or `:miss` on timeout/all-notfound/zero-peers; forgery guard
  enforced; resolve-once.
- **b2g parent-fetch** prefers P2P (mempool hits skip REST) then falls back to the existing RPC→JungleBus→
  WhatsOnChain cascade in order; `fetch_remote/1`'s result shape is unchanged.
- **Broadcast** behavior is byte-for-byte Phase 4 (all T4.2 tests unchanged) — Phase 5 only moves the routing
  decision into `SourceRouter`.
- **Cold-start safety (headline):** P2P primary + zero peers ⇒ raw-tx fetch and broadcast still succeed via
  REST/RPC fallback, and realtime still flows via ZMQ/SSE — proven at T5.3/T5.4/T5.5. `p2p_available?` is the
  only gate; the router never blocks on peers.
- No migration (Phase 5 adds no schema). Format clean under Elixir 1.15; app code clean under `mix compile
  --warnings-as-errors` (modulo the `bsv_sdk` path-dep skew).

---

## Suggested commit sequence
`feat(p2p): wire TxFetcher frame_sink + supervisor (T5.S)` → `feat(p2p): pure SourceRouter (T5.0)` →
`feat(p2p): pure TxFetcher tracker (T5.1)` → `feat(p2p): TxFetcher GenServer (T5.2)` →
`feat(p2p): route b2g parent-fetch through P2P-first router (T5.3)` →
`feat(p2p): broadcast + realtime via SourceRouter, cold-start safe (T5.4)` →
`test(p2p): b2g P2P fast-path integration (T5.5)` → `test(p2p): TxFetcher live smoke (T5.6)`.
