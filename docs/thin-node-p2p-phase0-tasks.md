# Phase 0 — Pure Codec: TDD Task Breakdown

Companion to `thin-node-p2p-plan.md`. Scope: the **pure, socket-free** BSV P2P wire codec
(`lib/athanor/p2p/frame.ex`, `lib/athanor/p2p/codec/*`, `lib/athanor/p2p/messages/*`).
No `:gen_tcp`, no GenServers. Everything here is deterministic input→output, so it is 100%
unit-testable offline with exact byte vectors.

## TDD discipline (every task)
1. **RED** — write the test(s) listed; run `mix test <file>`; confirm it fails *for the stated
   reason* (undefined function / wrong bytes), not a typo.
2. **GREEN** — minimal implementation to pass. No speculative generality.
3. **REFACTOR** — dedupe, name, doc the module header (per project rules), re-run green.
4. Commit per task (`feat(p2p): <task>`), no AI attribution.

## Dependencies / setup (do once, before T0.1)
- Add to `mix.exs` (test only): `{:stream_data, "~> 1.0", only: :test}` for property tests.
- Test layout: `test/athanor/p2p/` mirrors `lib/athanor/p2p/`.
- Fixtures: `test/support/p2p_vectors.ex` — module of known-answer byte vectors (see T0.14).
- All hashing uses `:crypto.hash(:sha256, ...)`; **no NIF needed in Phase 0.**

## Conventions the tests lock in
- **Endianness:** integers little-endian on the wire *except* `addr` port (uint16 **big-endian**).
- **Hash order:** P2P frames carry **wire-order** (raw double-SHA256 bytes). App uses
  **display-order** (reversed). Conversion is `Codec.Hash.wire_to_display/1` and back — tested in T0.3,
  used nowhere else in Phase 0 (messages keep raw 32-byte wire hashes; conversion happens at the boundary).
- Functions return `{:ok, value, rest_binary}` for readers (so they compose over a buffer) and a
  `binary` for writers. Decoders that can't yet decide return `:need_more`.

---

## T0.0 — Skeleton modules + failing smoke test
**RED:** `test/athanor/p2p/frame_test.exs` with one test calling `Athanor.P2P.Frame.encode/3` →
fails `UndefinedFunctionError`.
**GREEN:** create empty modules with `@moduledoc` headers and stub signatures raising.
**REFACTOR:** none. Establishes the tree compiles.

---

## T0.1 — CompactSize (varint) read/write — `Athanor.P2P.Codec.VarInt`
The single most reused primitive; get the boundaries exact.

**RED** — `codec/var_int_test.exs`, table of known answers for `write/1`:
| value | bytes |
|---|---|
| `0` | `<<0x00>>` |
| `252` | `<<0xFC>>` |
| `253` | `<<0xFD, 0xFD, 0x00>>` |
| `0xFFFF` | `<<0xFD, 0xFF, 0xFF>>` |
| `0x10000` | `<<0xFE, 0x00, 0x00, 0x01, 0x00>>` |
| `0xFFFFFFFF` | `<<0xFE, 0xFF, 0xFF, 0xFF, 0xFF>>` |
| `0x100000000` | `<<0xFF, 0,0,0,0, 1,0,0,0>>` |

`read/1` cases (return `{:ok, value, rest}`):
- each row above, with a trailing sentinel byte `<<0xAA>>` appended → asserts `rest == <<0xAA>>`.
- **truncation:** `read(<<0xFD, 0x01>>)` (prefix promises 2 bytes, only 1 present) → `:need_more`.
- empty input `read(<<>>)` → `:need_more`.

**GREEN:** pattern-match the 4 prefixes; `<<0xFD, n::little-16, rest::binary>>` etc.
**REFACTOR:** one private `encode_min/1` chooses smallest prefix.

---

## T0.2 — var_str & var_bytes — `Athanor.P2P.Codec.VarBytes`
**RED** — `codec/var_bytes_test.exs`:
- `write_bytes(<<>>)` → `<<0x00>>`.
- `write_bytes(<<1,2,3>>)` → `<<0x03, 1, 2, 3>>`.
- `write_str("/Athanor:0.1/")` → `<<13, "/Athanor:0.1/">>` (length-prefixed UTF-8).
- `read_bytes/1` round-trips; returns `{:ok, payload, rest}`; respects a `max` bound arg →
  `read_bytes(bin, max: 4)` on a length-prefix of 5 returns `{:error, :oversize}`.
- truncated payload (length says 3, only 2 bytes) → `:need_more`.

**GREEN:** compose VarInt + `binary-size(n)`.
**REFACTOR:** `read_str` is `read_bytes` then no transform (bytes are the string).

---

## T0.3 — checksum + hash order — `Athanor.P2P.Codec.Hash`
**RED** — `codec/hash_test.exs`:
- `double_sha256(<<>>)` first 4 bytes == `<<0x5D, 0xF6, 0xE0, 0xE2>>` (the canonical empty checksum).
- `checksum4(payload)` == first 4 bytes of `double_sha256(payload)` for a non-empty vector
  (use a known tx hash vector, e.g. `double_sha256(<<0x61>>)` "a" → assert full 32 against a
  precomputed constant placed in the fixture module).
- `wire_to_display(h)` reverses 32 bytes; `display_to_wire/1` is its inverse;
  `wire_to_display(display_to_wire(h)) == h` for a fixed 32-byte vector.
- guard: non-32-byte input to the order fns raises `FunctionClauseError`.

**GREEN:** `:crypto.hash` twice; `:binary.bin_to_list |> Enum.reverse` or `binary` reverse.
**REFACTOR:** keep order fns total over exactly 32 bytes (clause on `binary-size(32)`).

---

## T0.4 — network params — `Athanor.P2P.Network` (mainnet **and** testnet)
Athanor defaults to **testnet** (`config/runtime.exs:29`, `lib/athanor/blockchain/network.ex:37-40`),
so params are network-keyed from the start — no P2P code may hardcode mainnet.

**RED** — `network_test.exs`:
- `mainnet().magic == <<0xE3, 0xE1, 0xF3, 0xE8>>` (wire order), `mainnet().default_port == 8333`.
- `testnet().magic == <<0xF4, 0xE5, 0xF3, 0xF4>>` and `testnet().default_port == 18333`
  — **values to confirm against `bitcoin-sv` `chainparams.cpp` (Testnet3) in this task**; the test
  encodes the confirmed constants so any drift is caught.
- `for_network(:mainnet) == mainnet()` and `for_network(:testnet) == testnet()`; an unknown atom raises.
  (`for_network/1` is what the supervisor calls with Athanor's resolved network.)
- each network's `dns_seeds` is a non-empty list (mainnet: the three known hosts; testnet:
  `testnet-seed.bitcoinsv.io` et al — confirm); `fallback_seeds` is a non-empty list of `{ip, port}`
  per network (mirror `pnSeed6_main` / `pnSeed6_test`).
- `command_name(:version) == "version"`, padded form `padded_command(:verack) == <<"verack", 0,0,0,0,0,0>>`
  (exactly 12 bytes); unknown atom raises. (Command encoding is network-independent.)

**GREEN:** module attributes + small per-network maps + `for_network/1`.
**REFACTOR:** derive padding via `String.pad_trailing/3` on the binary; share the command table across networks.

---

## T0.5 — `Frame.encode/3`
`encode(network, command_atom, payload_binary) :: binary`

**RED** — `frame_test.exs` (encode group):
- **verack KAT:** `encode(mainnet, :verack, <<>>)` ==
  `<<0xE3,0xE1,0xF3,0xE8, "verack",0,0,0,0,0,0, 0,0,0,0, 0x5D,0xF6,0xE0,0xE2>>` (exact 24 bytes).
- header field positions for a non-empty payload `<<0xAB,0xCD>>` on `:ping`:
  - bytes 0..3 == magic; 4..15 == `"ping"` null-padded; 16..19 == `<<2,0,0,0>>` (len LE);
    20..23 == `checksum4(<<0xAB,0xCD>>)`; 24.. == payload.
- command > 12 chars → raises (we only have known commands; guard anyway).

**GREEN:** build `magic <> padded_cmd <> <<len::little-32>> <> checksum4 <> payload`.
**REFACTOR:** pull header assembly into a private fn.

---

## T0.6 — `Frame.decode/2` (streaming state machine) — the crux of Phase 0
`decode(network, buffer) :: {:ok, %Frame{command, payload}, rest} | :need_more | {:error, reason}`

**RED** — `frame_test.exs` (decode group). This is where most bugs live, so be exhaustive:
- **round-trip:** `decode(mainnet, encode(mainnet, :inv, p)) == {:ok, %Frame{command: "inv", payload: p}, <<>>}`.
- **need_more, header:** `decode(mainnet, <<0xE3,0xE1,0xF3,0xE8>>)` (only 4 of 24 header bytes) → `:need_more`.
- **need_more, payload:** a valid 24-byte header declaring len=10 followed by only 4 payload bytes → `:need_more`.
- **leftover/rest:** two concatenated verack frames → first `decode` returns the second frame as `rest`;
  decoding `rest` returns it cleanly (proves buffer composition).
- **bad magic:** flip a magic byte → `{:error, :bad_magic}`.
- **bad checksum:** valid frame with one checksum byte flipped → `{:error, :bad_checksum}`.
- **bad command:** non-ASCII/garbage in command field, or bytes after the first NUL that aren't NUL
  → `{:error, :bad_command}`.
- **oversized:** header declares `len > max_payload` (pass `max_payload: 32 * 1024 * 1024`) →
  `{:error, :oversized_payload}` *without* waiting for the bytes.

Example shape (illustrative):
```elixir
test "returns the trailing bytes as rest" do
  net = Network.mainnet()
  two = Frame.encode(net, :verack, <<>>) <> Frame.encode(net, :verack, <<>>)
  assert {:ok, %Frame{command: "verack"}, rest} = Frame.decode(net, two)
  assert byte_size(rest) == 24
  assert {:ok, %Frame{command: "verack"}, <<>>} = Frame.decode(net, rest)
end
```

**GREEN:** single `decode/3` with `max_payload` default; head pattern
`<<magic::binary-4, cmd::binary-12, len::little-32, csum::binary-4, rest::binary>>`; check magic,
then `len` vs max, then `byte_size(rest) < len → :need_more`, then split payload, verify checksum,
validate command, build struct.
**REFACTOR:** order the checks so cheap rejects (magic, oversize) precede hashing; extract
`validate_command/1`.

---

## T0.7 — `version` message — `Athanor.P2P.Messages.Version`
`serialize(%Version{}) :: binary` and `parse(binary) :: {:ok, %Version{}, rest}`

**RED** — `messages/version_test.exs`:
- field-order serialization for a fixed struct (protocol 70016, services 0, fixed timestamp,
  zeroed addr_recv/addr_from, fixed nonce, UA `"/Athanor:0.1.0/"`, start_height 0, relay true):
  assert the exact byte layout slice-by-slice:
  - `<<70016::little-32>>` at offset 0,
  - services `::little-64`, timestamp `::signed-little-64`,
  - 26-byte addr_recv = `<<services::little-64, ip::binary-16, port::big-16>>` (**port big-endian** — explicit test),
  - nonce `::little-64`, then var_str UA, `start_height::little-32`, `relay` 1 byte.
- **parse is lenient** (BSV/Bitcoin leniency): a payload truncated after `start_height` (no relay byte)
  still parses with `relay: true` default; a payload truncated after `nonce` (no UA) parses with `user_agent: ""`.
- round-trip for the full struct.
- protocol version constant exposed as `Version.protocol_version() == 70016`.

**GREEN:** straight `<<>>` builder; parser reads required prefix then optional tail guarded by `byte_size`.
**REFACTOR:** a private `addr_field/2` builds the 26-byte CAddress (reused by addr in T0.12).

---

## T0.8 — empty messages + `protoconf` — `Messages.Protoconf`
**RED** — `messages/protoconf_test.exs`:
- `verack`, `getaddr`, `mempool` have no message module — assert they encode via `Frame.encode(net, cmd, <<>>)`
  (a guard test that the command atoms exist in `Network`).
- `Protoconf.serialize(%Protoconf{max_recv_payload: 0x02000000, policies: "Default"})` ==
  `VarInt.write(2) <> <<0x02000000::little-32>> <> VarBytes.write_str("Default")`.
- `parse/1` round-trips; a 1-field protoconf (no policies) parses with `policies: ""`.
- default constructor yields `max_recv_payload == 33_554_432` (32 MiB) — the big-tx ceiling from the plan.

**GREEN/REFACTOR:** trivial compose.

---

## T0.9 — inv / getdata / notfound — `Messages.Inv` (+ `InvVector`)
Shared structure: `VarInt(count) <> count * (<<type::little-32>> <> hash::binary-32)`. Types: tx=1, block=2.

**RED** — `messages/inv_test.exs`:
- `InvVector` type atoms map: `type_code(:tx)==1`, `type_code(:block)==2`; unknown raises.
- serialize a 2-item inv (one tx, one block, with fixed 32-byte wire hashes) → exact bytes:
  `<<2>> <> <<1::little-32>> <> h1 <> <<2::little-32>> <> h2`.
- parse round-trips and preserves order and **wire-order hashes unchanged**.
- count guard: parse with declared count 3 but only 1 item present → `:need_more`.
- oversize guard: declared count beyond a sane cap (pass `max_items`) → `{:error, :oversize}`.
- `getdata`/`notfound` reuse the same serializer (just different command at frame layer) — one test asserts identical body bytes for the same items.

**GREEN:** recursive/`Enum` builder + reader looping `count` times accumulating `rest`.
**REFACTOR:** generic `read_items(bin, count, max, reader_fn)` reused by headers/addr.

---

## T0.10 — getheaders / headers — `Messages.Headers`
**RED** — `messages/headers_test.exs`:
- `GetHeaders.serialize(%{version: 70016, locator: [h1, h2], stop: h0})` ==
  `<<70016::little-32>> <> VarInt.write(2) <> h1 <> h2 <> h0` (locator hashes are 32-byte wire order; stop is 32 bytes, all-zero when "to tip").
- locator cap: > 101 locator hashes → `{:error, :too_many_locators}` on serialize (MAX_LOCATOR_SZ).
- `Headers.parse` of a `headers` body: `VarInt(count) <> count * (80-byte header <> VarInt(txcount=0))`.
  - parse a 1-header body → `{:ok, [%BlockHeader{raw: <<80 bytes>>}], <<>>}`; assert the trailing
    `00` tx-count byte is consumed.
  - **tx-count must be 0** in headers msgs; a non-zero tx-count → `{:error, :bad_headers}` (matches node behavior).
  - count cap 2000 → oversize error beyond it.
- `BlockHeader` helper: `prev_hash/1` extracts bytes 4..35 (and exposes them display-order via Hash);
  `hash/1` == `double_sha256(raw)` (wire order). Test against a real mainnet header vector (T0.14).

**GREEN:** compose; `BlockHeader` is a struct wrapping the raw 80 bytes + lazy hash.
**REFACTOR:** reuse `read_items` from T0.9.

---

## T0.11 — reject — `Messages.Reject`
**RED** — `messages/reject_test.exs`:
- serialize `%Reject{message: "tx", ccode: 0x10, reason: "bad", data: h}` ==
  `VarBytes.write_str("tx") <> <<0x10>> <> VarBytes.write_str("bad") <> h` (data present for tx/block).
- parse round-trips; a reject with no trailing data (ccode for a non-tx/block) parses `data: <<>>`.
- `classify/1` maps ccode/reason → atom (`:policy | :invalid | :conflicted | :transient | :unknown`)
  per a small table (mirror consigliere's `RejectClass`); unknown reason → `:unknown`.

**GREEN/REFACTOR:** straight compose + a `classify` lookup.

---

## T0.12 — addr — `Messages.Addr`
**RED** — `messages/addr_test.exs`:
- serialize a 1-entry addr: `VarInt(1) <> <<time::little-32>> <> <<services::little-64, ip::binary-16, port::big-16>>`
  (note: **with** the 4-byte time field, unlike the version addr fields which omit nTime).
- parse round-trips; extracts `{ip, port}` tuples for the pool (T-Phase-2 consumer).
- count cap (1000) → oversize error.
- IPv4-mapped-IPv6 helper: `ipv4_to_16(<<a,b,c,d>>)` == `<<0::80, 0xFFFF::16, a,b,c,d>>`; inverse extracts the v4.

**GREEN/REFACTOR:** reuse `addr_field/2` from T0.7 (without nTime) and add the nTime-prefixed variant here.

---

## T0.13 — property tests (round-trip invariants) — `messages/roundtrip_prop_test.exs`
Using `stream_data`:
- **VarInt:** `∀ n in 0..0xFFFFFFFFFFFFFFFF: read(write(n)) == {:ok, n, <<>>}` and `write` picks the
  minimal length (assert byte_size matches the boundary table).
- **VarBytes:** `∀ bin: read_bytes(write_bytes(bin)) == {:ok, bin, <<>>}`.
- **Frame:** `∀ cmd in known_commands, payload (bounded ≤ 1KB): decode(net, encode(net, cmd, payload)) == {:ok, %Frame{command: to_string(cmd), payload: payload}, <<>>}`.
- **Frame streaming:** `∀ frame bytes, split point k: decode(net, take(k)) ∈ {:need_more}` and decoding the
  full buffer succeeds — i.e. any prefix shorter than a full frame is `:need_more`, never a false `:ok`.
- **inv/headers/addr:** `∀ list of items (bounded): parse(serialize(list)) == {:ok, list, <<>>}` with hashes unchanged.

These catch endianness, off-by-one, and partial-buffer bugs the KATs miss.

---

## T0.14 — real captured-frame conformance vector — `conformance_test.exs`
This is the "is our codec actually wire-correct" gate (DXS's canonical vector idea).

**Capture once** (document the command in the test module header):
```
# one-liner to grab a real version frame from a live BSV peer (run manually, paste hex into fixture):
#   printf '<our version frame hex>' | xxd -r -p | nc <peer_ip> 8333 | xxd -p | head
# or use a known-good vector from bitcoin-sv functional test data.
```
Put the captured hex blobs in `test/support/p2p_vectors.ex` as module functions, **per network**
(`testnet_version_frame/0`, `mainnet_version_frame/0`, `testnet_block_header/0`,
`mainnet_block_header/0`, …). Testnet is the default and the cheapest to capture (port 18333), so it
is the required vector; mainnet is captured too so the magic/seed divergence is exercised.

**RED** tests (run for **both** networks):
- `decode(testnet(), testnet_version_frame())` → `{:ok, %Frame{command: "version"}, <<>>}` and
  `Version.parse(frame.payload)` recovers a sane `start_height` (> 1_600_000 on testnet3 / > 800_000 on
  mainnet), a `user_agent` starting with `/`, protocol ≥ 70015. Same for the mainnet vector with
  `mainnet()` — and a **cross-network negative**: `decode(mainnet(), testnet_version_frame())` →
  `{:error, :bad_magic}` (proves magic is actually enforced per network).
- `Headers.parse(<network>_headers_body())` yields the expected header count and the first header's
  `hash/1` equals the known block hash (display-order) for that height on that network.
- `BlockHeader.hash(<network>_block_header())` equals a hardcoded known block id.

**GREEN:** by this point the codec should already pass these; if not, the real vector exposes the bug
the synthetic KATs hid (usually hash order or a leniency gap). Fix minimally, re-run.

---

## Definition of Done (Phase 0)
- All T0.1–T0.14 green; `mix test test/athanor/p2p` clean.
- `mix format` + project doc-header rule satisfied on every new module.
- Zero socket/GenServer code introduced (grep: no `:gen_tcp`, no `use GenServer` under `lib/athanor/p2p` yet).
- `Frame.decode/2` handles all six error/needs states with explicit tests (the Phase-1 peer loop depends on this contract being exact).
- Captured real `version` + a real header verified — proves wire-correctness before any networking exists.

## Suggested commit sequence
`T0.0 skeleton → T0.1 varint → T0.2 varbytes → T0.3 hash → T0.4 network → T0.5 encode →
T0.6 decode → T0.7 version → T0.8 protoconf → T0.9 inv → T0.10 headers → T0.11 reject →
T0.12 addr → T0.13 props → T0.14 conformance`.
Each is a standalone failing-test-first commit; T0.6 and T0.14 are the two highest-risk and deserve the most cases.
