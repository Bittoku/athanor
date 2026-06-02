defmodule Athanor.P2P.NetworkTest do
  use ExUnit.Case, async: true

  alias Athanor.P2P.Network

  describe "per-network parameters (from bitcoin-sv chainparams.cpp netMagic)" do
    test "mainnet magic and default port" do
      assert Network.mainnet().magic == <<0xE3, 0xE1, 0xF3, 0xE8>>
      assert Network.mainnet().default_port == 8333
    end

    test "testnet magic and default port" do
      assert Network.testnet().magic == <<0xF4, 0xE5, 0xF3, 0xF4>>
      assert Network.testnet().default_port == 18333
    end

    test "each network advertises DNS seeds; fallback_seeds is a list" do
      for params <- [Network.mainnet(), Network.testnet()] do
        assert is_list(params.dns_seeds) and params.dns_seeds != []
        assert Enum.all?(params.dns_seeds, &is_binary/1)
        assert is_list(params.fallback_seeds)
      end

      assert "seed.bitcoinsv.io" in Network.mainnet().dns_seeds
      assert "testnet-seed.bitcoinsv.io" in Network.testnet().dns_seeds
    end
  end

  describe "for_network/1" do
    test "resolves Athanor's network atoms to the matching params" do
      assert Network.for_network(:mainnet) == Network.mainnet()
      assert Network.for_network(:testnet) == Network.testnet()
    end

    test "raises on an unknown network" do
      assert_raise FunctionClauseError, fn -> Network.for_network(:dogenet) end
    end
  end

  describe "command encoding (network-independent)" do
    test "command_name/1 returns the wire command string" do
      assert Network.command_name(:version) == "version"
      assert Network.command_name(:verack) == "verack"
    end

    test "padded_command/1 NUL-pads the command to exactly 12 bytes" do
      assert Network.padded_command(:verack) == <<"verack", 0, 0, 0, 0, 0, 0>>
      assert byte_size(Network.padded_command(:version)) == 12
    end

    test "raises on an unknown command" do
      assert_raise FunctionClauseError, fn -> Network.command_name(:frobnicate) end
    end
  end
end
