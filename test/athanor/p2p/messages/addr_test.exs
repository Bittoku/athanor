defmodule Athanor.P2P.Messages.AddrTest do
  use ExUnit.Case, async: true

  alias Athanor.P2P.Messages.Addr
  alias Athanor.P2P.Codec.VarInt

  describe "IPv4-in-IPv6 mapping" do
    test "ipv4_to_16/1 produces the ::ffff:a.b.c.d form and is invertible" do
      assert Addr.ipv4_to_16(<<10, 0, 0, 1>>) == <<0::80, 0xFFFF::16, 10, 0, 0, 1>>
      assert Addr.ipv4_from_16(Addr.ipv4_to_16(<<10, 0, 0, 1>>)) == {:ok, <<10, 0, 0, 1>>}
    end

    test "ipv4_from_16/1 returns :error for a non-mapped address" do
      assert Addr.ipv4_from_16(:binary.copy(<<0xAB>>, 16)) == :error
    end
  end

  describe "serialize/1 and parse/1" do
    test "round-trips an entry: time LE-32 + (services LE-64, ip, port BIG-16)" do
      ip = Addr.ipv4_to_16(<<127, 0, 0, 1>>)
      entry = {1_700_000_000, 1, ip, 8333}

      assert Addr.serialize([entry]) ==
               VarInt.write(1) <>
                 <<1_700_000_000::little-32, 1::little-64>> <> ip <> <<8333::big-16>>

      assert Addr.parse(Addr.serialize([entry])) == {:ok, [entry], <<>>}
    end

    test ":need_more when an entry is incomplete" do
      bin = VarInt.write(1) <> <<0::little-32>>
      assert Addr.parse(bin) == :need_more
    end

    test "{:error, :oversize} when the count exceeds the 1000 cap" do
      assert Addr.parse(VarInt.write(2000)) == {:error, :oversize}
    end
  end
end
