defmodule Athanor.P2P.Codec.VarIntTest do
  use ExUnit.Case, async: true

  alias Athanor.P2P.Codec.VarInt

  describe "write/1" do
    test "encodes values at every CompactSize boundary" do
      assert VarInt.write(0) == <<0x00>>
      assert VarInt.write(252) == <<0xFC>>
      assert VarInt.write(253) == <<0xFD, 0xFD, 0x00>>
      assert VarInt.write(0xFFFF) == <<0xFD, 0xFF, 0xFF>>
      assert VarInt.write(0x10000) == <<0xFE, 0x00, 0x00, 0x01, 0x00>>
      assert VarInt.write(0xFFFFFFFF) == <<0xFE, 0xFF, 0xFF, 0xFF, 0xFF>>
      assert VarInt.write(0x100000000) == <<0xFF, 0, 0, 0, 0, 1, 0, 0, 0>>
    end
  end

  describe "read/1" do
    test "decodes each boundary value and returns the trailing bytes as rest" do
      for value <- [0, 252, 253, 0xFFFF, 0x10000, 0xFFFFFFFF, 0x100000000] do
        encoded = VarInt.write(value)
        assert VarInt.read(encoded <> <<0xAA>>) == {:ok, value, <<0xAA>>}
      end
    end

    test "returns :need_more when a multi-byte prefix promises more bytes than present" do
      assert VarInt.read(<<0xFD, 0x01>>) == :need_more
    end

    test "returns :need_more on empty input" do
      assert VarInt.read(<<>>) == :need_more
    end
  end
end
