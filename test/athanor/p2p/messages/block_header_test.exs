defmodule Athanor.P2P.Messages.BlockHeaderTest do
  @moduledoc """
  Tests for `Athanor.P2P.Messages.BlockHeader` accessors (Phase 6 T6.S adds the
  wire-order parent + compact-bits accessors the headers tree needs). The raw 80
  bytes are `version(4) ++ prev(32) ++ merkle(32) ++ timestamp(4) ++ bits(4) ++
  nonce(4)`.
  """
  use ExUnit.Case, async: true

  alias Athanor.P2P.Messages.BlockHeader

  defp header(prev_wire, bits_le) do
    raw =
      <<1::little-32>> <>
        prev_wire <>
        :binary.copy(<<0x22>>, 32) <>
        <<0::little-32>> <>
        bits_le <>
        <<0::little-32>>

    %BlockHeader{raw: raw}
  end

  test "prev_hash_wire/1 returns the raw prev_block bytes (wire order, no reversal)" do
    prev = :binary.copy(<<0xAB>>, 32)
    h = header(prev, <<0xFF, 0xFF, 0x00, 0x1D>>)
    assert BlockHeader.prev_hash_wire(h) == prev
    # ...and it is the byte-reverse of the display-order prev_hash/1.
    assert BlockHeader.prev_hash(h) == Athanor.P2P.Codec.Hash.wire_to_display(prev)
  end

  test "bits/1 reads the 4-byte little-endian compact target" do
    # 0x1d00ffff stored little-endian is <<0xff, 0xff, 0x00, 0x1d>>.
    h = header(:binary.copy(<<0>>, 32), <<0xFF, 0xFF, 0x00, 0x1D>>)
    assert BlockHeader.bits(h) == 0x1D00FFFF
  end

  test "timestamp/1 reads the 4-byte little-endian timestamp (header bytes 68..71)" do
    # version(4) ++ prev(32) ++ merkle(32) ++ timestamp(4) ++ bits(4) ++ nonce(4).
    # 0x495FAB29 = 1231006505, the Bitcoin genesis block time.
    raw =
      <<1::little-32>> <>
        :binary.copy(<<0xAB>>, 32) <>
        :binary.copy(<<0x22>>, 32) <>
        <<0x495FAB29::little-32>> <>
        <<0xFF, 0xFF, 0x00, 0x1D>> <>
        <<0::little-32>>

    assert BlockHeader.timestamp(%BlockHeader{raw: raw}) == 0x495FAB29
  end
end
