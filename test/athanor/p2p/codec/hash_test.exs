defmodule Athanor.P2P.Codec.HashTest do
  use ExUnit.Case, async: true

  alias Athanor.P2P.Codec.Hash

  describe "double_sha256/1" do
    test "matches the known-answer double-SHA256 of the empty string" do
      # SHA256(SHA256("")) — a fixed external test vector.
      expected =
        <<0x5D, 0xF6, 0xE0, 0xE2, 0x76, 0x13, 0x59, 0xD3, 0x0A, 0x82, 0x75, 0x05, 0x8E, 0x29,
          0x9F, 0xCC, 0x03, 0x81, 0x53, 0x45, 0x45, 0xF5, 0x5C, 0xF4, 0x3E, 0x41, 0x98, 0x3F,
          0x5D, 0x4C, 0x94, 0x56>>

      assert Hash.double_sha256(<<>>) == expected
    end
  end

  describe "checksum4/1" do
    test "is the first four bytes of the double-SHA256 of the payload" do
      payload = "the quick brown fox"
      assert Hash.checksum4(payload) == binary_part(Hash.double_sha256(payload), 0, 4)
    end

    test "empty-payload checksum is the canonical 0x5df6e0e2" do
      assert Hash.checksum4(<<>>) == <<0x5D, 0xF6, 0xE0, 0xE2>>
    end
  end

  describe "wire/display order conversion" do
    test "wire_to_display reverses the 32 bytes and is invertible" do
      wire = :binary.list_to_bin(Enum.to_list(1..32))
      display = Hash.wire_to_display(wire)

      assert display == :binary.list_to_bin(Enum.to_list(32..1//-1))
      assert Hash.display_to_wire(display) == wire
    end

    test "raises on non-32-byte input" do
      assert_raise FunctionClauseError, fn -> Hash.wire_to_display(<<1, 2, 3>>) end
      assert_raise FunctionClauseError, fn -> Hash.display_to_wire(<<1, 2, 3>>) end
    end
  end
end
