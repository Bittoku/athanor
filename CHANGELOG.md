# Changelog

All notable changes to Athanor are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] — unreleased (pending tag)

The headline of this release is a **native-Elixir BSV P2P "thin-node" layer**, so
the indexer can run P2P-primary with REST/RPC as a fallback instead of depending
solely on external history providers. The P2P stack is **disabled by default**
(`config :athanor, Athanor.P2P, enabled: true` to turn it on); when off, behavior
is unchanged from 0.1.0.

### Added

- **P2P wire layer** — a from-scratch BSV peer-to-peer implementation in
  `lib/athanor/p2p/` (no external P2P dependency): message/frame codec, single-peer
  connect + version/verack handshake, and a peer pool with DNS-seed + addr-gossip
  discovery (with /24 address diversity).
- **Mempool observer** — folds peer `inv → getdata → tx` through a request-lifecycle
  tracker, verifies each tx by re-hashing the payload, prefilters against the
  watched-address/token set, and indexes matches. Every observation is
  **source-tagged** (`:zmq | :junglebus | :whatsonchain | :block | :p2p`) in
  `metadata["sources"]`, with cross-source de-duplication.
- **Transaction relay + P2P-primary broadcast** — `broadcast_tx/2` announces over
  P2P and falls back to RPC/REST, with a monotonic status lattice
  (`pending < relayed < unconfirmed < rejected < accepted < propagated`) and an
  audit trail.
- **Headers chain + reorg detection** — a bounded headers tree that chooses the best
  tip by **cumulative work** (never height) and detects reorgs via the
  common-ancestor walk.
- **Consensus cw-144 DAA validation** — P2P-learned headers are validated against the
  Bitcoin-Cash Nov-2017 difficulty-adjustment algorithm (mirroring bitcoin-abc
  `GetNextCashWorkRequired`/`GetSuitableBlock`/`ComputeTarget`), pinned by mainnet
  golden vectors. Non-canonical / easier-than-consensus headers are rejected
  (fail-closed). Mainnet only; testnet is an explicit fail-closed stub.
- **P2P-driven index integration** — a single tip-authority controller
  (`Athanor.Indexer.TipController`) applies chain changes under an
  *advisory-until-RPC-confirmed* model: P2P/ZMQ/JungleBus tips are hints that
  trigger an RPC-confirmed reconcile (rollback + reapply by hash), keeping the index
  contiguous across catch-up and reorg. The legacy `ChainTipVerifier` RPC poller is
  retired in favor of this single mutation owner.
- **STAS 3.0 v0.1 §10.2 P2MPKH** support in the indexer, plus STAS 3.0 token
  redemption detection.
- Expanded unit-test coverage across the indexer, infra, services, and workers
  layers, with injectable HTTP transport (`Req.Test`) and fetcher seams.
- `RpcClient.get_block_header/2` (uses `getblockheader` — small even for large
  blocks).

### Changed

- `Athanor.Blockchain.Network` exposes a pure `resolve/0` (config reader) so the P2P
  network — and therefore the armed DAA gate — derives from the authoritative
  application network (`config :athanor, :network`).
- Dependencies refreshed so `mix.lock` is reproducible (`mix deps.get
  --check-locked` passes), including **`req` 0.5 → 0.6** which pulls upstream
  security fixes (multipart header-injection and decompression-bomb DoS). Athanor's
  JSON-only HTTP usage is unaffected functionally.

### Fixed

- Propagate `illegal_roots` taint through STAS3 transfer chains; free spent parent
  UTXOs on reorg rollback; STAS3 address-history and balance gaps.
- Token/satoshi balance sums could leave a `Decimal` unconverted (`Repo.one()`
  returning `%Decimal{}`/`nil`); now normalized to integer.
- One malformed transaction output no longer aborts output scanning and drops
  matches from sibling outputs in the same tx.
- WalletChannel reply-timeout flake (the `assert_reply` default 100ms was too tight
  under full-suite load).

## [0.1.0]

- Initial Athanor indexer: STAS 3.0 token indexing, UTXO/address tracking, balance
  and provenance services, the wallet WebSocket channel, and ingestion via ZMQ /
  JungleBus / WhatsOnChain.
