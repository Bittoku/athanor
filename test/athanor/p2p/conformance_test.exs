defmodule Athanor.P2P.ConformanceTest do
  @moduledoc """
  Wire-correctness gate (T0.14): decode real captured/canonical bytes, not just
  our own re-encoded output. If a synthetic KAT hid a byte-order or leniency
  bug, these real vectors expose it.
  """
  use ExUnit.Case, async: true

  alias Athanor.P2P.{Frame, Network, Vectors}
  alias Athanor.P2P.Codec.Hash
  alias Athanor.P2P.Messages.{BlockHeader, Headers, Version}

  describe "real captured testnet version frame" do
    test "decodes as a version frame and parses sane fields" do
      frame = Vectors.testnet_version_frame()

      assert {:ok, %Frame{command: "version", payload: payload}, <<>>} =
               Frame.decode(Network.testnet(), frame)

      # Real BSV version messages append a trailing `association_id` (a var_bytes)
      # after `relay`. Our lenient parser intentionally stops at `relay` (the
      # BSV association field is out of Phase-0 scope), so it leaves that trailer
      # as `rest`. Here the trailer is <<0>> (an empty association id).
      assert {:ok, %Version{} = v, trailer} = Version.parse(payload)
      assert trailer == <<0>>
      assert v.version >= 70_015
      assert v.start_height > 1_600_000
      assert String.starts_with?(v.user_agent, "/")
      assert v.user_agent =~ "Bitcoin SV"
    end

    test "is rejected as :bad_magic when decoded with the wrong network" do
      assert Frame.decode(Network.mainnet(), Vectors.testnet_version_frame()) ==
               {:error, :bad_magic}
    end
  end

  describe "canonical genesis block headers" do
    test "mainnet genesis header hashes to the known block id" do
      bh = %BlockHeader{raw: Vectors.mainnet_genesis_header()}
      assert Hash.wire_to_display(BlockHeader.hash(bh)) == Vectors.mainnet_genesis_id()
    end

    test "testnet3 genesis header hashes to the known block id" do
      bh = %BlockHeader{raw: Vectors.testnet_genesis_header()}
      assert Hash.wire_to_display(BlockHeader.hash(bh)) == Vectors.testnet_genesis_id()
    end

    test "Headers.parse of a real one-header body recovers the genesis header" do
      raw = Vectors.mainnet_genesis_header()
      # a headers message body: count(1) + (80-byte header + tx_count 0)
      body = <<1>> <> raw <> <<0>>

      assert {:ok, [%BlockHeader{raw: ^raw} = bh], <<>>} = Headers.parse(body)
      assert Hash.wire_to_display(BlockHeader.hash(bh)) == Vectors.mainnet_genesis_id()
    end
  end
end
