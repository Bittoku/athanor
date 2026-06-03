defmodule Athanor.P2P.Messages.VersionTest do
  use ExUnit.Case, async: true

  alias Athanor.P2P.Messages.Version

  defp fixture do
    %Version{
      version: 70_016,
      services: 0,
      timestamp: 1_700_000_000,
      addr_recv: Version.net_addr(0, <<0::128>>, 0),
      addr_from: Version.net_addr(0, <<0::128>>, 0),
      nonce: 0x0102030405060708,
      user_agent: "/Athanor:0.1.0/",
      start_height: 0,
      relay: true
    }
  end

  test "protocol_version/0 is 70016" do
    assert Version.protocol_version() == 70_016
  end

  describe "net_addr/3" do
    test "is 26 bytes: services LE-64, 16-byte ip, port BIG-endian-16" do
      addr = Version.net_addr(1, <<0::80, 0xFFFF::16, 127, 0, 0, 1>>, 8333)
      assert byte_size(addr) == 26
      assert binary_part(addr, 0, 8) == <<1::little-64>>
      assert binary_part(addr, 24, 2) == <<8333::big-16>>
    end
  end

  describe "serialize/1" do
    test "lays out fields in wire order" do
      bin = Version.serialize(fixture())

      assert binary_part(bin, 0, 4) == <<70_016::little-32>>
      assert binary_part(bin, 4, 8) == <<0::little-64>>
      assert binary_part(bin, 12, 8) == <<1_700_000_000::signed-little-64>>
      # addr_recv (26) at 20, addr_from (26) at 46, both zeroed
      assert binary_part(bin, 20, 26) == <<0::208>>
      assert binary_part(bin, 46, 26) == <<0::208>>
      # nonce at 72
      assert binary_part(bin, 72, 8) == <<0x0102030405060708::little-64>>
      # user_agent varstr at 80
      assert binary_part(bin, 80, 16) == <<15, "/Athanor:0.1.0/">>
      # start_height (4) then relay (1)
      assert binary_part(bin, 96, 4) == <<0::little-32>>
      assert binary_part(bin, 100, 1) == <<1>>
    end
  end

  describe "parse/1" do
    test "round-trips a full version payload" do
      v = fixture()
      assert Version.parse(Version.serialize(v)) == {:ok, v, <<>>}
    end

    test "is lenient: missing relay byte defaults relay to true" do
      full = Version.serialize(fixture())
      without_relay = binary_part(full, 0, byte_size(full) - 1)
      assert {:ok, %Version{relay: true}, <<>>} = Version.parse(without_relay)
    end

    test "is lenient: missing user_agent / start_height / relay" do
      # truncate right after the 8-byte nonce (offset 80)
      prefix = binary_part(Version.serialize(fixture()), 0, 80)

      assert {:ok, %Version{user_agent: "", start_height: 0, relay: true}, <<>>} =
               Version.parse(prefix)
    end

    test ":need_more when the fixed 80-byte prefix is incomplete" do
      # 40 zero bytes — a valid binary shorter than the 80-byte fixed prefix
      assert Version.parse(<<0::320>>) == :need_more
    end
  end
end
