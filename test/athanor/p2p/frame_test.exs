defmodule Athanor.P2P.FrameTest do
  use ExUnit.Case, async: true

  alias Athanor.P2P.{Frame, Network}
  alias Athanor.P2P.Codec.Hash

  describe "encode/3" do
    test "verack known-answer vector (empty payload, canonical checksum)" do
      expected =
        <<0xE3, 0xE1, 0xF3, 0xE8, "verack", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x5D, 0xF6, 0xE0, 0xE2>>

      assert Frame.encode(Network.mainnet(), :verack, <<>>) == expected
      assert byte_size(Frame.encode(Network.mainnet(), :verack, <<>>)) == 24
    end

    test "header field layout for a non-empty payload" do
      payload = <<0xAB, 0xCD>>
      frame = Frame.encode(Network.mainnet(), :ping, payload)

      assert binary_part(frame, 0, 4) == <<0xE3, 0xE1, 0xF3, 0xE8>>
      assert binary_part(frame, 4, 12) == <<"ping", 0, 0, 0, 0, 0, 0, 0, 0>>
      assert binary_part(frame, 16, 4) == <<2, 0, 0, 0>>
      assert binary_part(frame, 20, 4) == Hash.checksum4(payload)
      assert binary_part(frame, 24, byte_size(payload)) == payload
    end

    test "uses the network's magic" do
      assert binary_part(Frame.encode(Network.testnet(), :verack, <<>>), 0, 4) ==
               <<0xF4, 0xE5, 0xF3, 0xF4>>
    end

    test "raises on an unknown command" do
      assert_raise FunctionClauseError, fn ->
        Frame.encode(Network.mainnet(), :frobnicate, <<>>)
      end
    end
  end
end
