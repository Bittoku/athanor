defmodule Athanor.P2P.Messages.ProtoconfTest do
  use ExUnit.Case, async: true

  alias Athanor.P2P.Messages.Protoconf
  alias Athanor.P2P.Codec.{VarBytes, VarInt}

  test "default max_recv_payload is 32 MiB (the big-tx ceiling)" do
    default = %Protoconf{}
    assert default.max_recv_payload == 33_554_432
  end

  describe "serialize/1" do
    test "two-field form: numberOfFields=2, max LE-32, then stream policies var_str" do
      p = %Protoconf{max_recv_payload: 0x02000000, policies: "Default"}

      assert Protoconf.serialize(p) ==
               VarInt.write(2) <> <<0x02000000::little-32>> <> VarBytes.write_str("Default")
    end

    test "one-field form when policies are empty: numberOfFields=1, no policy bytes" do
      p = %Protoconf{max_recv_payload: 0x02000000, policies: ""}
      assert Protoconf.serialize(p) == VarInt.write(1) <> <<0x02000000::little-32>>
    end
  end

  describe "parse/1" do
    test "round-trips the two-field form" do
      p = %Protoconf{max_recv_payload: 0x01000000, policies: "Default"}
      assert Protoconf.parse(Protoconf.serialize(p)) == {:ok, p, <<>>}
    end

    test "one-field protoconf parses policies as empty" do
      p = %Protoconf{max_recv_payload: 0x01000000, policies: ""}
      assert Protoconf.parse(Protoconf.serialize(p)) == {:ok, p, <<>>}
    end
  end
end
