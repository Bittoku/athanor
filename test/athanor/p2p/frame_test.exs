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

  describe "decode/3" do
    test "round-trips an encoded frame and returns empty rest" do
      payload = <<1, 2, 3, 4, 5>>
      encoded = Frame.encode(Network.mainnet(), :inv, payload)

      assert Frame.decode(Network.mainnet(), encoded) ==
               {:ok, %Frame{command: "inv", payload: payload}, <<>>}
    end

    test ":need_more when the buffer is shorter than the 24-byte header" do
      assert Frame.decode(Network.mainnet(), <<0xE3, 0xE1, 0xF3, 0xE8>>) == :need_more
    end

    test ":need_more when the declared payload has not fully arrived" do
      # valid header declaring len=10, but only 4 payload bytes follow
      header =
        <<0xE3, 0xE1, 0xF3, 0xE8>> <>
          Network.padded_command(:tx) <>
          <<10::little-32>> <> Hash.checksum4(<<>>) <> <<1, 2, 3, 4>>

      assert Frame.decode(Network.mainnet(), header) == :need_more
    end

    test "returns the trailing bytes of a second frame as rest" do
      net = Network.mainnet()
      two = Frame.encode(net, :verack, <<>>) <> Frame.encode(net, :verack, <<>>)

      assert {:ok, %Frame{command: "verack"}, rest} = Frame.decode(net, two)
      assert byte_size(rest) == 24
      assert {:ok, %Frame{command: "verack"}, <<>>} = Frame.decode(net, rest)
    end

    test "{:error, :bad_magic} on a wrong magic" do
      <<_b, tail::binary>> = Frame.encode(Network.mainnet(), :verack, <<>>)
      assert Frame.decode(Network.mainnet(), <<0x00, tail::binary>>) == {:error, :bad_magic}
    end

    test "{:error, :bad_checksum} when the checksum does not match the payload" do
      <<head::binary-20, _csum::binary-4, payload::binary>> =
        Frame.encode(Network.mainnet(), :inv, <<9, 9>>)

      corrupted = head <> <<0xDE, 0xAD, 0xBE, 0xEF>> <> payload
      assert Frame.decode(Network.mainnet(), corrupted) == {:error, :bad_checksum}
    end

    test "{:error, :bad_command} when bytes after the first NUL are non-zero" do
      bad_command = <<"inv", 0, 0, 0, 0, 0, 0, 0, 0, 1>>

      frame =
        <<0xE3, 0xE1, 0xF3, 0xE8>> <> bad_command <> <<0::little-32>> <> Hash.checksum4(<<>>)

      assert Frame.decode(Network.mainnet(), frame) == {:error, :bad_command}
    end

    test "{:error, :oversized_payload} without waiting for the bytes" do
      header =
        <<0xE3, 0xE1, 0xF3, 0xE8>> <>
          Network.padded_command(:tx) <>
          <<0xFFFFFFFF::little-32>> <> Hash.checksum4(<<>>)

      assert Frame.decode(Network.mainnet(), header, max_payload: 32 * 1024 * 1024) ==
               {:error, :oversized_payload}
    end
  end
end
