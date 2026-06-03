defmodule Athanor.P2P.Messages.HeadersTest do
  use ExUnit.Case, async: true

  alias Athanor.P2P.Messages.{BlockHeader, Headers}
  alias Athanor.P2P.Codec.{Hash, VarInt}

  @h0 :binary.copy(<<0>>, 32)
  @h1 :binary.copy(<<0x11>>, 32)
  @h2 :binary.copy(<<0x22>>, 32)
  @header80 :binary.copy(<<0x5A>>, 80)

  describe "serialize_get_headers/3" do
    test "version, locator count, locator hashes, then stop hash" do
      assert Headers.serialize_get_headers(70_016, [@h1, @h2], @h0) ==
               <<70_016::little-32>> <> VarInt.write(2) <> @h1 <> @h2 <> @h0
    end

    test "allows exactly 101 locators but rejects 102 (MAX_LOCATOR_SZ)" do
      assert is_binary(Headers.serialize_get_headers(70_016, List.duplicate(@h1, 101), @h0))

      assert Headers.serialize_get_headers(70_016, List.duplicate(@h1, 102), @h0) ==
               {:error, :too_many_locators}
    end
  end

  describe "parse/2 (headers message)" do
    test "parses a one-header body and consumes the zero tx-count" do
      body = VarInt.write(1) <> @header80 <> VarInt.write(0)
      assert Headers.parse(body) == {:ok, [%BlockHeader{raw: @header80}], <<>>}
    end

    test "{:error, :bad_headers} when a header's tx-count is non-zero" do
      body = VarInt.write(1) <> @header80 <> VarInt.write(1)
      assert Headers.parse(body) == {:error, :bad_headers}
    end

    test ":need_more when a header is incomplete" do
      body = VarInt.write(1) <> binary_part(@header80, 0, 40)
      assert Headers.parse(body) == :need_more
    end

    test "{:error, :oversize} when the declared count exceeds the 2000 cap" do
      assert Headers.parse(VarInt.write(3000)) == {:error, :oversize}
    end
  end

  describe "BlockHeader" do
    test "hash/1 is the wire-order double-SHA256 of the 80 raw bytes" do
      assert BlockHeader.hash(%BlockHeader{raw: @header80}) == Hash.double_sha256(@header80)
    end

    test "prev_hash/1 extracts bytes 4..35 in display order" do
      prev = :binary.copy(<<0x11>>, 32)
      raw = <<1, 2, 3, 4>> <> prev <> :binary.copy(<<0>>, 44)
      assert BlockHeader.prev_hash(%BlockHeader{raw: raw}) == Hash.wire_to_display(prev)
    end
  end
end
