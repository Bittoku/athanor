# Athanor Thin-Node (BSV P2P) — Design & Implementation Plan

Status: DRAFT / for review · Date: 2026-06-01 · Author: CLU (analysis of `dxs-consigliere@codex/consigliere-vnext`)

> **Revision — 2026-06-02 (review round 1, Hermes/dave):** added testnet network params
> (Athanor defaults to testnet), bound the P2P watchlist to reuse `TransactionFilter.matches?/1`,
> added the source-tagging/dedupe contract (§5a) and the integration contracts to specify
> before their phase (§10), and resolved the open decisions in §9. Fixed unmatched code fences.

## 0. Goal & honest scope

Remove Athanor's **hard real-time dependency on JungleBus / a full BSV node** by adding a
native BSV P2P client as the *primary* source for the live path, while keeping REST
providers (Bitails / WhatsOnChain / JungleBus) as explicit **fallbacks** for the jobs that
P2P physically cannot do.

This mirrors what DXS shipped in `consigliere-vnext`. Their own design docs are blunt about
the limit, and we adopt the same honest posture:

> "P2P `getdata` reliably serves only mempool + recent tx; peers do NOT serve arbitrary
> confirmed/historical txids (those live in blocks)." — thin-node-primary-source-wave

### What P2P genuinely replaces (no external service needed)
- Real-time **mempool observation** (`inv(MSG_TX)` → `getdata` → `tx`).
- Raw bytes of **mempool-resident** transactions.
- **Broadcast** (+ propagation proof via relay-back).
- **Chain-tip / headers** signal and reorg detection (Phase 2).

### What still requires a REST provider (by design — do not pretend otherwise)
- **Back-to-genesis ancestor traversal** (`b2g_resolver.ex`) — confirmed parents live in blocks.
- **Confirmed/historical raw tx by txid.**
- **Historical address / token scans** (need an address-indexed API → Bitails).
- **Block backfill** of bodies (GB-scale over P2P; deferred by DXS too).
- **Cold-start headers anchor** (seed tip height/hash from REST, then advance on P2P).

Net: this is a "P2P-primary, REST-fallback" indexer, not a pure P2P node. That is the
correct and achievable target.

### Network posture (testnet-first — matches the repo default)
Athanor defaults to **testnet** (`config/runtime.exs:29` defaults `NETWORK` to `"testnet"`;
`lib/athanor/blockchain/network.ex:37-40` maps config → `:mainnet` / `:testnet`). Therefore
the P2P client is **network-parameterized from Phase 0** — magic bytes, default port, and DNS
seeds are selected from `Athanor.P2P.Network` keyed off the same resolved network, not
hardcoded to mainnet. Phase 0 ships and tests **both** testnet and mainnet params; Phase 1's
live smoke test defaults to testnet and treats mainnet as opt-in. No P2P code path may assume
mainnet. (Resolves §9.1.)

## 1. Build-vs-wrap decision (recommended)

**Hybrid:**
- **Networking = native Elixir** (`:gen_tcp`, binary pattern-matching, per-peer GenServers).
  Rationale: this is OTP's wheelhouse; per-peer fault isolation and pool refill are free via
  supervisors. A Rust P2P crate behind a NIF would block BEAM schedulers on long-lived
  sockets (needs dirty schedulers/ports) and forfeit the supervision story.
- **Script / STAS3 truth = reuse the Rust engine via the existing `bsv_sdk` NIF**
  (`{:bsv_sdk, path: "../bsv_sdk_elixir"}` is already in `mix.exs`). Do NOT rebuild the
  interpreter DXS had to write — you already own the canonical engine. NIFs are right here
  (CPU-bound, deterministic, no blocking IO) — exactly where DXS spent effort on restored
  BSV opcodes (`OP_BIN2NUM`) and sighash correctness.

## 2. Current Athanor topology (what we're slotting into)

```
blockchain/jungle_bus_client.ex   ← JungleBus WS (realtime)      ┐ replace as PRIMARY
blockchain/zmq_listener.ex        ← node ZMQ rawtx/hashblock     ┘ (keep as fallback)
blockchain/rpc_client.ex          ← bitcoind JSON-RPC            keep (fallback/optional)
infra/bitails.ex                  ← REST                         keep (fallback)
infra/whats_on_chain.ex           ← REST                         keep (fallback)
indexer/block_processor.ex        ← consumes blocks
indexer/transaction_processor.ex  ← consumes txs
indexer/transaction_filter.ex     ← watchlist match (matches?/1 is the source of truth)
indexer/b2g_resolver.ex           ← back-to-genesis (STAYS on REST)
indexer/utxo_manager.ex           ← UTXO state
workers/chain_tip_verifier.ex     ← tip tracking (P2P feeds this in Phase 2)
workers/unconfirmed_monitor.ex    ← mempool lifecycle
```

Key architectural move (copied from consigliere): introduce a **capability router** so
"which source serves X" is config, not hardcoded. Athanor's existing ingest already fans
into `transaction_processor`/`block_processor`; the P2P client becomes just another
publisher into that same pipeline.

## 3. Target module layout (new code, native Elixir)

```
lib/athanor/p2p/
├── supervisor.ex          # Athanor.P2P.Supervisor — owns the tree below (placement: see §10.1)
├── network.ex             # per-network magic bytes, default port, DNS + fallback seeds (mainnet AND testnet)
├── frame.ex               # PURE codec: encode/1, decode/1 :: {:ok, frame, rest}|:need_more|{:error,_}
├── messages/              # pure (de)serializers
│   ├── version.ex         #   version/verack + protocol 70016 + BSV protoconf
│   ├── inv.ex             #   inv / getdata / notfound (shared InvVector)
│   ├── headers.ex         #   getheaders (locator) / headers
│   ├── reject.ex          #   reject + class mapping
│   └── addr.ex            #   addr gossip
├── peer.ex                # GenServer: one TCP conn; handshake → recv loop → dispatch
├── peer_pool.ex           # GenServer: maintain N peers, seed discovery, /24 diversity, cooldown
├── peer_registry.ex       # Registry of live peers (keyed by host:port)
├── discovery.ex           # DNS seed resolve + addr-gossip absorb + fallback seeds
├── mempool_observer.ex    # inv(MSG_TX) → dedup(:ets,TTL) → ratelimit → getdata → verify → match → emit
├── watchlist.ex           # :ets hash160 PREFILTER ONLY; final inclusion delegates to TransactionFilter.matches?/1
├── headers_chain.ex       # Phase 2: ≤N-header window + reorg (cumulative work)
├── tx_relay.ex            # broadcast: inv to peers, serve getdata, relay-back counter
└── source_router.ex       # capability → {primary, [fallbacks]} (realtime/raw_tx/validation/...)
```

`Athanor.P2P.Supervisor` is added gated by config so it can be turned off (default off until
soak-tested). Its exact placement (child of `Blockchain.Supervisor` under `:rest_for_one`, vs.
a sibling supervisor) is decided with tests in §10.1.

## 4. Wire protocol cheat-sheet (so the codec is unambiguous)

- **Frame:** `<<magic::little-32, command::binary-12, len::little-32, checksum::binary-4, payload::binary-size(len)>>`
  - command = ASCII, NUL-padded to 12.
  - checksum = first 4 bytes of `:crypto.hash(:sha256, :crypto.hash(:sha256, payload))`.
- **Networks (parameterized; testnet is Athanor's default — confirm all values against
  `bitcoin-sv` `chainparams.cpp` and Athanor's `network.ex` in T0.4):**
  | | mainnet | testnet (Testnet3) |
  |---|---|---|
  | magic (wire bytes) | `e3 e1 f3 e8` | `f4 e5 f3 f4` *(verify)* |
  | default port | 8333 | 18333 *(verify)* |
  | DNS seeds | `seed.bitcoinsv.io`, `seed.satoshisvision.network`, `seed.bitcoinseed.directory` | `testnet-seed.bitcoinsv.io` et al *(verify)* |
  | fallback IPs (Phase 2) | mirror `pnSeed6_main` | mirror `pnSeed6_test` |

  Note: hardcoded fallback IPs are populated in **Phase 2 (discovery)**, not Phase 0 — DNS seeds +
  `addr` gossip are the self-refreshing bootstrap; the bitcoin-sv IP tables go stale (revised per MR !2 review).
- **Handshake:** send `version` → read frames until {peer version recv ∧ peer verack recv ∧ our verack sent}; 30s timeout. BSV-specific: send `protoconf` right after our verack (advertise max recv payload; raise to 32 MiB for big mainnet txs).
- **Hash order trap:** every hash in P2P frames is **wire order** (LE, raw double-SHA256 output). Athanor's stores/REST use **display order** (byte-reversed). Centralize a `wire_to_display/1` + `display_to_wire/1` and convert ONLY at the P2P boundary. (This bit DXS repeatedly — see their `TxHashOrder`.)

## 5. Phased plan (bottom-up; each phase independently verifiable)

### Phase 0 — Pure codec (no sockets) ✅ verifiable offline
- `frame.ex` encode/decode + `messages/version.ex`, `inv.ex`, `headers.ex`, `reject.ex`, `addr.ex`.
- Network params for **both** mainnet and testnet (`network.ex`).
- **Verify:** unit tests against a *captured real frame* for **each network** (grab a `version` payload
  from a `/Bitcoin SV/` peer, like DXS's canonical conformance vector). Round-trip encode∘decode == identity.

### Phase 1 — Single peer + handshake
- `peer.ex` GenServer: `:gen_tcp.connect` (`active: :once`, `nodelay: true`), inline handshake,
  then steady recv loop accumulating a binary buffer through `Frame.decode/1`.
- ping/pong keepalive; reject/timeout → terminate (supervisor restarts).
- **Verify:** connect to one known testnet node (default), log a completed handshake + an inbound `inv`.
  Mainnet smoke is the same test, opt-in via `--only external` (see Phase 1 task doc).

### Phase 2 — Peer pool + discovery
- `peer_pool.ex` + `discovery.ex` + `peer_registry.ex`: maintain target 8 peers, DNS+fallback
  seed bootstrap, absorb `addr` gossip, /24 subnet diversity, negative cooldown (~15 min) on failure.
- **Verify:** pool self-heals to N after killing a peer; no two peers share a /24.

### Phase 2.5 — Source tagging + dedupe contract (pre-Phase-3) — *added per review*
Before P2P publishes into the shared pipeline, decide and implement how an observation's
**source/provider** (`:p2p | :zmq | :junglebus | :bitails`) is carried and how duplicate arrivals
(same txid via P2P and an existing path) are reconciled. The current pipeline has **no source field**:
- `TransactionFilter.process_raw_tx/1` — `lib/athanor/indexer/transaction_filter.ex:89-90`
- forwards `{:index_tx, tx, matched_addresses, matched_tokens}` — `…/transaction_filter.ex:143-146`
- `TransactionProcessor` call/cast handlers take no source arg — `…/transaction_processor.ex:31-32, 43-50`
- `MetaTransaction` has `metadata` but no explicit source field — `…/schema/meta_transaction.ex:17-32`

**Decision (recommended, confirm with owner):** carry source as an optional 4th element threaded
through `process_raw_tx`/`{:index_tx, …}` and recorded in `MetaTransaction.metadata["sources"]`
(a set, append-on-dedupe) plus per-source metrics/logging — **no schema migration** in this phase.
First-seen wins for indexing; later duplicates only union the source set + update lag metrics.
- **Tests:** same txid arriving via P2P and via the existing path → indexed once, `metadata["sources"]`
  contains both, no double-spend/double-index; ordering-independent (either source first).

### Phase 3 — Mempool observation → indexer
- `mempool_observer.ex`: `inv(MSG_TX)` → `:ets` dedup (TTL ~600s) → token-bucket rate-limit
  (~200/s) → `getdata(MSG_TX)` → on `tx`, re-hash payload to verify txid → parse →
  **`watchlist.ex` prefix PREFILTER → final inclusion via `TransactionFilter.matches?/1`** →
  publish into existing `transaction_processor` with source `:p2p` (per §2.5).
- **Watchlist is a prefilter only.** The `:ets` hash160 8-byte-prefix index exists solely to cheaply
  reject obviously-irrelevant txs at P2P ingest rate. Authoritative inclusion (P2PKH + STAS/STAS3
  semantics) MUST reuse `Athanor.Indexer.TransactionFilter.matches?/1`
  (`…/transaction_filter.ex:93-105, 167-241`) so P2P, ZMQ, and JungleBus paths cannot diverge.
  (Resolves §9.3.) The full mempool request lifecycle contract is specified in §10.2.
- **Verify:** a watched-address payment seen via P2P lands in the same store as the JungleBus path;
  dedupe across both sources works (display-order txid); a tx that passes the prefix prefilter but
  fails `matches?/1` is correctly dropped (prefilter never widens inclusion).

### Phase 4 — Broadcast + relay-back
- `tx_relay.ex`: announce `inv(MSG_TX)` to N−2 peers, wire getdata/inv/reject handlers on ALL
  peers; serve `getdata` from a pending-tx map; mark propagated when ≥2 peers relay the inv back.
- Fold into existing `services/broadcast.ex` as the primary broadcast path; node/REST broadcast as fallback.
- **Verify:** self-broadcast a tx round-trips (we see our own txid come back via a non-target peer).

### Phase 5 — Capability router + make P2P primary
- **Start with a direct-call inventory** (§10.3) before introducing `source_router.ex` — e.g. broadcast
  currently calls `RpcClient.send_raw_transaction` directly at `lib/athanor/services/broadcast.ex:38-49`.
- `source_router.ex`: `resolve(:realtime_ingest|:raw_tx_fetch|:broadcast|:validation_fetch|...)`
  → `{primary, fallbacks}` from config. Defaults: realtime/raw_tx/broadcast → `:p2p` primary,
  REST fallbacks; **validation_fetch / block_backfill / historical_scan stay REST** (honest).
- Raw-tx fetch: P2P `getdata` with a short (~3s) timeout → `null` on miss → REST fallback loop
  (Bitails/WoC/JungleBus). Wire into `b2g_resolver.ex`'s parent-fetch so mempool hits skip REST.
- **Verify:** with P2P primary AND zero peers connected, ingest still works via fallback (cold-start safety — DXS's named rule).

### Phase 6 (optional) — Headers chain + reorg from P2P
- `headers_chain.ex`: seed tip from REST once, then advance on `inv(MSG_BLOCK)` + `getheaders`;
  ≤N-header window; reorg via prev-block walk + **cumulative work** (not height) comparison.
- Feed `workers/chain_tip_verifier.ex` from this instead of JungleBus/RPC.
- **Verify:** induce a 1-block fork on testnet; detector emits correct orphan/connect sets.

## 6. STAS3 validation — reuse, don't rebuild
For any tx that needs script-level STAS3 truth (issuance-set gate, five-flag propagation,
preimage checks), call the existing `bsv_sdk` NIF (Rust engine). This is the single place a
NIF is clearly correct. Do not port consigliere's `bsv-native-interpreter`; you already have
the canonical engine that the SDK exposes.

## 7. Risks / caveats to track
- **Cold start:** keep REST realtime runner alive in parallel until the P2P pool warms (DXS rule).
- **Peer acceptance:** some nodes UA-filter; keep fallback seeds + don't advertise a banned UA.
- **Deep reorg (> window):** enter a degraded state requiring operator action; alert on it.
- **Watchlist scale:** `:ets` prefix index is fine to ~hundreds of k addresses; revisit (bloom / shard) beyond ~1M.
- **Big txs:** raise max recv payload (protoconf) to 32 MiB or large mainnet txs get dropped.
- **Hash-order bugs:** the #1 recurring defect — keep the wire/display conversion at exactly one boundary.

## 8. Rough effort
- Phases 0–3 (the real win: self-sufficient live mempool): ~the bulk of value, moderate.
- Phase 4–5 (broadcast + router): small-to-moderate, mostly glue into existing services.
- Phase 6 (headers/reorg): moderate, optional.
- Net: Elixir/OTP makes this materially less code than the C# original; binary matching shrinks the codec.

## 9. Open decisions — resolved in this revision
1. **Network:** testnet-first (matches repo default); P2P is network-parameterized from Phase 0,
   both testnet + mainnet params shipped/tested. (See §0 "Network posture", §4 table.)
2. **JungleBus WS:** keep as a parallel redundant realtime observer until P2P soaks green (DXS default);
   retirement is a later, metrics-driven call. *(Confirm with owner — only open question remaining.)*
3. **Watchlist source of truth:** P2P uses an `:ets` prefix prefilter, but final inclusion reuses
   `TransactionFilter.matches?/1`. No second matching implementation. (See §3, §5 Phase 3.)

## 10. Integration contracts — specify before the owning phase (non-blocking, per review)
1. **Supervision placement (before Phase 1 wiring).** Current fixed tree is
   `Network → RpcClient → ZmqListener` under `lib/athanor/blockchain/supervisor.ex:20-27`. Decide with
   tests: P2P supervisor as a child of `Blockchain.Supervisor` (`:rest_for_one`) vs. a sibling supervisor;
   plus tests for config-key defaults, child inclusion/exclusion when disabled, and restart behavior.
2. **Mempool request lifecycle contract (before Phase 3).** Specify: which peer receives `getdata`,
   outstanding-request tracking, `notfound` handling, request timeout, peer-disconnect mid-request,
   duplicate `tx` arrival, and backpressure under inv floods. Each gets explicit tests.
3. **Phase 5 direct-call inventory (before `source_router`).** Enumerate existing direct provider calls
   (e.g. `services/broadcast.ex:38-49` → `RpcClient.send_raw_transaction`) so routing is introduced
   against a known call-site map rather than ad hoc.
