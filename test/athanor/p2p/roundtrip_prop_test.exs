defmodule Athanor.P2P.RoundtripPropTest do
  @moduledoc """
  Property-based round-trip invariants over the pure P2P wire codec (T0.13).

  These catch endianness, off-by-one, and partial-buffer bugs that the
  known-answer tests in earlier tasks might miss.
  """
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Athanor.P2P.{Frame, Network}
  alias Athanor.P2P.Codec.{VarBytes, VarInt}
  alias Athanor.P2P.Messages.{Addr, BlockHeader, Headers, Inv}

  @u16 0xFFFF
  @u32 0xFFFFFFFF
  @u64 0xFFFFFFFFFFFFFFFF

  @commands [
    :version,
    :verack,
    :ping,
    :pong,
    :protoconf,
    :inv,
    :getdata,
    :notfound,
    :reject,
    :addr,
    :getaddr,
    :headers,
    :getheaders,
    :block,
    :tx,
    :mempool,
    :sendheaders,
    :feefilter
  ]

  property "VarInt round-trips and always uses the minimal width" do
    check all(n <- integer(0..@u64)) do
      encoded = VarInt.write(n)
      assert VarInt.read(encoded) == {:ok, n, <<>>}

      expected_width =
        cond do
          n < 0xFD -> 1
          n <= @u16 -> 3
          n <= @u32 -> 5
          true -> 9
        end

      assert byte_size(encoded) == expected_width
    end
  end

  property "VarBytes round-trips arbitrary payloads" do
    check all(payload <- binary()) do
      assert VarBytes.read_bytes(VarBytes.write_bytes(payload)) == {:ok, payload, <<>>}
    end
  end

  property "Frame encode∘decode is identity for any known command + payload" do
    net = Network.mainnet()

    check all(
            command <- member_of(@commands),
            payload <- binary(max_length: 1024)
          ) do
      expected = {:ok, %Frame{command: Network.command_name(command), payload: payload}, <<>>}
      assert Frame.decode(net, Frame.encode(net, command, payload)) == expected
    end
  end

  property "every proper prefix of a frame decodes to :need_more, never a false :ok" do
    net = Network.mainnet()

    check all(payload <- binary(max_length: 64)) do
      frame = Frame.encode(net, :tx, payload)

      for split <- 0..(byte_size(frame) - 1) do
        assert Frame.decode(net, binary_part(frame, 0, split)) == :need_more
      end
    end
  end

  property "inv serialize/parse round-trips, preserving order and wire hashes" do
    item = tuple({member_of([:tx, :block]), binary(length: 32)})

    check all(items <- list_of(item, max_length: 20)) do
      assert Inv.parse(Inv.serialize(items)) == {:ok, items, <<>>}
    end
  end

  property "addr serialize/parse round-trips" do
    entry =
      tuple({integer(0..@u32), integer(0..@u64), binary(length: 16), integer(0..@u16)})

    check all(entries <- list_of(entry, max_length: 20)) do
      assert Addr.parse(Addr.serialize(entries)) == {:ok, entries, <<>>}
    end
  end

  property "a headers body parses back to its BlockHeader list" do
    check all(raws <- list_of(binary(length: 80), max_length: 10)) do
      body =
        VarInt.write(length(raws)) <>
          Enum.map_join(raws, fn raw -> raw <> VarInt.write(0) end)

      expected = Enum.map(raws, fn raw -> %BlockHeader{raw: raw} end)
      assert Headers.parse(body) == {:ok, expected, <<>>}
    end
  end
end
