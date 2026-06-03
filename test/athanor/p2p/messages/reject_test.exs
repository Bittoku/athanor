defmodule Athanor.P2P.Messages.RejectTest do
  use ExUnit.Case, async: true

  alias Athanor.P2P.Messages.Reject
  alias Athanor.P2P.Codec.VarBytes

  @h :binary.copy(<<0xCC>>, 32)

  describe "serialize/1" do
    test "message var_str, ccode byte, reason var_str, then data" do
      r = %Reject{message: "tx", ccode: 0x10, reason: "bad", data: @h}

      assert Reject.serialize(r) ==
               VarBytes.write_str("tx") <> <<0x10>> <> VarBytes.write_str("bad") <> @h
    end
  end

  describe "parse/1" do
    test "round-trips a tx reject carrying a 32-byte txid" do
      r = %Reject{message: "tx", ccode: 0x10, reason: "bad", data: @h}
      assert Reject.parse(Reject.serialize(r)) == {:ok, r, <<>>}
    end

    test "a non-tx/block reject has empty data" do
      r = %Reject{message: "ping", ccode: 0x01, reason: "nope", data: <<>>}
      assert Reject.parse(Reject.serialize(r)) == {:ok, r, <<>>}
    end

    test ":need_more when a tx reject's 32-byte data is incomplete" do
      bin = VarBytes.write_str("tx") <> <<0x10>> <> VarBytes.write_str("bad") <> <<0, 0, 0>>
      assert Reject.parse(bin) == :need_more
    end
  end

  describe "classify/1" do
    test "maps ccode/reason to a reject class" do
      mk = fn code, reason -> %Reject{message: "tx", ccode: code, reason: reason, data: <<>>} end

      assert Reject.classify(mk.(0x42, "insufficient fee")) == :policy
      assert Reject.classify(mk.(0x10, "bad-txns")) == :invalid
      assert Reject.classify(mk.(0x12, "txn-mempool-conflict")) == :conflicted
      assert Reject.classify(mk.(0x99, "???")) == :unknown
    end
  end
end
