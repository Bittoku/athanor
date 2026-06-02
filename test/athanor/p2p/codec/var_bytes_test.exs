defmodule Athanor.P2P.Codec.VarBytesTest do
  use ExUnit.Case, async: true

  alias Athanor.P2P.Codec.VarBytes

  describe "write_bytes/1" do
    test "length-prefixes an empty payload" do
      assert VarBytes.write_bytes(<<>>) == <<0x00>>
    end

    test "length-prefixes a short payload" do
      assert VarBytes.write_bytes(<<1, 2, 3>>) == <<0x03, 1, 2, 3>>
    end
  end

  describe "write_str/1" do
    test "encodes a string as length-prefixed UTF-8 bytes" do
      assert VarBytes.write_str("/Athanor:0.1/") == <<13, "/Athanor:0.1/">>
    end
  end

  describe "read_bytes/2" do
    test "round-trips and returns the unconsumed remainder" do
      assert VarBytes.read_bytes(VarBytes.write_bytes(<<1, 2, 3>>) <> <<0xAA>>) ==
               {:ok, <<1, 2, 3>>, <<0xAA>>}
    end

    test "returns :need_more when the declared length exceeds the available bytes" do
      # length prefix says 3, only 2 payload bytes present
      assert VarBytes.read_bytes(<<0x03, 1, 2>>) == :need_more
    end

    test "rejects a payload whose declared length exceeds :max" do
      # length prefix says 5, over the max of 4
      assert VarBytes.read_bytes(<<0x05, 1, 2, 3, 4, 5>>, max: 4) == {:error, :oversize}
    end
  end

  describe "read_str/2" do
    test "round-trips a string" do
      assert VarBytes.read_str(VarBytes.write_str("hi")) == {:ok, "hi", <<>>}
    end
  end
end
