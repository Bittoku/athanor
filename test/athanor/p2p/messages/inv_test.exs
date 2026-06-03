defmodule Athanor.P2P.Messages.InvTest do
  use ExUnit.Case, async: true

  alias Athanor.P2P.Messages.Inv

  @h1 :binary.copy(<<0xAA>>, 32)
  @h2 :binary.copy(<<0xBB>>, 32)

  describe "type codes" do
    test "tx=1, block=2; unknown atom raises; integers pass through" do
      assert Inv.type_code(:tx) == 1
      assert Inv.type_code(:block) == 2
      assert Inv.type_code(7) == 7
      assert_raise FunctionClauseError, fn -> Inv.type_code(:weird) end

      assert Inv.type_from_code(1) == :tx
      assert Inv.type_from_code(2) == :block
      assert Inv.type_from_code(7) == 7
    end
  end

  describe "serialize/1" do
    test "count-prefixed (type LE-32, 32-byte hash) items" do
      items = [{:tx, @h1}, {:block, @h2}]

      assert Inv.serialize(items) ==
               <<2>> <> <<1::little-32>> <> @h1 <> <<2::little-32>> <> @h2
    end
  end

  describe "parse/2" do
    test "round-trips, preserving order and wire-order hashes" do
      items = [{:tx, @h1}, {:block, @h2}]
      assert Inv.parse(Inv.serialize(items)) == {:ok, items, <<>>}
    end

    test "returns the unconsumed remainder as rest" do
      items = [{:tx, @h1}]
      assert Inv.parse(Inv.serialize(items) <> <<0xEE>>) == {:ok, items, <<0xEE>>}
    end

    test ":need_more when fewer items than the declared count" do
      bin = <<3>> <> <<1::little-32>> <> @h1
      assert Inv.parse(bin) == :need_more
    end

    test "{:error, :oversize} when the declared count exceeds :max_items" do
      assert Inv.parse(<<5>>, max_items: 4) == {:error, :oversize}
    end
  end
end
