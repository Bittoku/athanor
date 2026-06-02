# Athanor Thin-Node (BSV P2P) — Design & Implementation Plan

Status: DRAFT / for review · Date: 2026-06-01 · Author: CLU (analysis of `dxs-consigliere@codex/consigliere-vnext`)

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
indexer/transaction_filter.ex     ← watchlist match
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
├── supervisor.ex          # Athanor.P2P.Supervisor — owns the tree below
├── network.ex             # magic bytes, default port, DNS + fallback seeds (mainnet/testnet)
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
├── mempool_observer.ex    # inv(MSG_TX) → dedup(:ets,TTL) → ratelimit → getdata → verify → emit
├── watchlist.ex           # :ets hash160 prefix index; hot-reload from DB
├── headers_chain.ex       # Phase 2: ≤N-header window + reorg (cumulative work)
├── tx_relay.ex            # broadcast: inv to peers, serve getdata, relay-back counter
└── source_router.ex       # capability → {primary, [fallbacks]} (realtime/raw_tx/validation/...)
```

`Athanor.P2P.Supervisor` is added as a child of the existing blockchain supervisor, gated by
config so it can be turned off (default off until soak-tested).

## 4. Wire protocol cheat-sheet (so the codec is unambiguous)

- **Frame:** `<<magic::little-32, command::binary-12, len::little-32, checksum::binary-4, payload::binary-size(len)>>`
  - mainnet magic bytes on wire: `e3 e1 f3 e8`; command = ASCII, NUL-padded to 12.
  - checksum = first 4 bytes of `:crypto.hash(:sha256, :crypto.hash(:sha256, payload))`.
- **Handshake:** send `version` → read frames until {peer version recv ∧ peer verack recv ∧ our verack sent}; 30s timeout. BSV-specific: send `protoconf` right after our verack (advertise max recv payload; raise to 32 MiB for big mainnet txs).
- **Hash order trap:** every hash in P2P frames is **wire order** (LE, raw double-SHA256 output). Athanor's stores/REST use **display order** (byte-reversed). Centralize a `wire_to_display/1` + `display_to_wire/1` and convert ONLY at the P2P boundary. (This bit DXS repeatedly — see their `TxHashOrder`.)
- **Seeds:** DNS — `seed.bitcoinsv.io`, `seed.satoshisvision.network`, `seed.bitcoinseed.directory`; plus ~8 hardcoded fallback IPs (mirror bitcoin-sv `pnSeed6_main`). Default port 8333.

## 5. Phased plan (bottom-up; each phase independently verifiable)

### Phase 0 — Pure codec (no sockets) ✅ verifiable offline
- `frame.ex` encode/decode + `messages/version.ex`, `inv.ex`, `headers.ex`, `reject.ex`, `addr.ex`.
- **Verify:** unit tests against a *captured real frame* (grab one `version` payload from a
  `/Bitcoin SV/` peer, like DXS's canonical conformance vector). Round-trip encode∘decode == identity.

### Phase 1 — Single peer + handshake
- `peer.ex` GenServer: `:gen_tcp.connect` (`active: :once`, `nodelay: true`), inline handshake,
  then steady recv loop accumulating a binary buffer through `Frame.decode/1`.
- ping/pong keepalive; reject/timeout → terminate (supervisor restarts).
- **Verify:** connect to one known mainnet node, log a completed handshake + an inbound `inv`.

### Phase 2 — Peer pool + discovery
- `peer_pool.ex` + `discovery.ex` + `peer_registry.ex`: maintain target 8 peers, DNS+fallback
  seed bootstrap, absorb `addr` gossip, /24 subnet diversity, negative cooldown (~15 min) on failure.
- **Verify:** pool self-heals to N after killing a peer; no two peers share a /24.

### Phase 3 — Mempool observation → indexer
- `mempool_observer.ex`: `inv(MSG_TX)` → `:ets` dedup (TTL ~600s) → token-bucket rate-limit
  (~200/s) → `getdata(MSG_TX)` → on `tx`, re-hash payload to verify txid → parse → `watchlist.ex`
  match (hash160 8-byte prefix index) → publish into existing `transaction_processor`.
- Mark source = `:p2p` on the observation (mirror consigliere's per-source tagging for metrics).
- **Verify:** a watched-address payment seen via P2P lands in the same store as the JungleBus path; dedupe across both sources works (display-order txid).

### Phase 4 — Broadcast + relay-back
- `tx_relay.ex`: announce `inv(MSG_TX)` to N−2 peers, wire getdata/inv/reject handlers on ALL
  peers; serve `getdata` from a pending-tx map; mark propagated when ≥2 peers relay the inv back.
- Fold into existing `services/broadcast.ex` as the primary broadcast path; node/REST broadcast as fallback.
- **Verify:** self-broadcast a tx round-trips (we see our own txid come back via a non-target peer).

### Phase 5 — Capability router + make P2P primary
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

## 9. Open decisions for the user
1. Mainnet-only first, or testnet harness for Phase 6 reorg testing too?
2. Keep JungleBus WS as a parallel redundant realtime observer (DXS default) or retire it once P2P soaks green?
3. Watchlist source of truth — reuse Athanor's existing tracked-address store directly, or a P2P-local mirror?
```
