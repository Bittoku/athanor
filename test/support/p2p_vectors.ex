defmodule Athanor.P2P.Vectors do
  @moduledoc """
  Real, externally-verifiable BSV wire vectors for the Phase-0 conformance
  tests (T0.14). These are not synthetic — they come from the live network and
  the canonical chain, so they prove the codec is wire-correct, not merely
  self-consistent.

  - `testnet_version_frame/0` — a full `version` frame captured live from a
    `/Bitcoin SV:1.2.2/` testnet (port 18333) peer.
  - `*_genesis_header/0` / `*_genesis_id/0` — the canonical genesis block
    headers and their display-order block ids (mainnet and testnet3).
  """

  # Captured 2026-06 from a testnet3 peer (magic f4 e5 f3 f4); UA
  # "/Bitcoin SV:1.2.2/", protocol 70016, start_height 0x1a8a82 (1,739,394).
  @testnet_version_frame_hex "f4e5f3f476657273696f6e000000000069000000e8aa3097801101002100000000000000ed07226a00000000000000000000000000000000000000000000ffff6a482f60fb262100000000000000000000000000000000000000000000000000ad987e8e753732bf122f426974636f696e2053563a312e322e322f828a1a000100"

  # Canonical genesis block headers (80 bytes) and their display-order ids.
  @mainnet_genesis_header_hex "0100000000000000000000000000000000000000000000000000000000000000000000003ba3edfd7a7b12b27ac72c3e67768f617fc81bc3888a51323a9fb8aa4b1e5e4a29ab5f49ffff001d1dac2b7c"
  @mainnet_genesis_id_hex "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"

  @testnet_genesis_header_hex "0100000000000000000000000000000000000000000000000000000000000000000000003ba3edfd7a7b12b27ac72c3e67768f617fc81bc3888a51323a9fb8aa4b1e5e4adae5494dffff001d1aa4ae18"
  @testnet_genesis_id_hex "000000000933ea01ad0ee984209779baaec3ced90fa3f408719526f8d77f4943"

  defp h(hex), do: Base.decode16!(hex, case: :lower)

  @doc "A real, full testnet `version` frame (24-byte header + 105-byte payload)."
  def testnet_version_frame, do: h(@testnet_version_frame_hex)

  @doc "The 80-byte mainnet genesis block header."
  def mainnet_genesis_header, do: h(@mainnet_genesis_header_hex)

  @doc "The mainnet genesis block id (display order, 32 bytes)."
  def mainnet_genesis_id, do: h(@mainnet_genesis_id_hex)

  @doc "The 80-byte testnet3 genesis block header."
  def testnet_genesis_header, do: h(@testnet_genesis_header_hex)

  @doc "The testnet3 genesis block id (display order, 32 bytes)."
  def testnet_genesis_id, do: h(@testnet_genesis_id_hex)
end
