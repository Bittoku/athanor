defmodule Athanor.Blockchain.NetworkTest do
  @moduledoc """
  Covers the `Network` GenServer: env-driven mainnet/testnet/stn parsing
  and the address-version-byte helper.

  Test mode (`skip_runtime_children: true`) means the global supervisor
  doesn't start `Network`, so each test starts its own under the
  production name via `start_supervised!/1`.
  """

  use ExUnit.Case, async: false

  alias Athanor.Blockchain.Network

  setup do
    Application.delete_env(:athanor, :network)
    on_exit(fn -> Application.delete_env(:athanor, :network) end)
    :ok
  end

  defp start_network!, do: start_supervised!(Network)

  ## ── Tests ──

  describe "network/0 — env parsing" do
    test "defaults to :mainnet when no env is set" do
      start_network!()
      assert Network.network() == :mainnet
    end

    test "returns :mainnet when env is the literal 'mainnet'" do
      Application.put_env(:athanor, :network, "mainnet")
      start_network!()
      assert Network.network() == :mainnet
    end

    test "returns :testnet when env is 'testnet'" do
      Application.put_env(:athanor, :network, "testnet")
      start_network!()
      assert Network.network() == :testnet
    end

    test "returns :testnet when env is 'stn' (Scaling Testnet)" do
      Application.put_env(:athanor, :network, "stn")
      start_network!()
      assert Network.network() == :testnet
    end

    test "treats any unknown env value as :mainnet" do
      Application.put_env(:athanor, :network, "regtest-mystery")
      start_network!()
      assert Network.network() == :mainnet
    end
  end

  describe "is_mainnet? / is_testnet?" do
    test "is_mainnet?/0 is true on default" do
      start_network!()
      assert Network.is_mainnet?() == true
      assert Network.is_testnet?() == false
    end

    test "is_testnet?/0 is true on testnet env" do
      Application.put_env(:athanor, :network, "testnet")
      start_network!()
      assert Network.is_testnet?() == true
      assert Network.is_mainnet?() == false
    end
  end

  describe "address_version/0" do
    test "returns 0x00 on mainnet (P2PKH)" do
      start_network!()
      assert Network.address_version() == 0x00
    end

    test "returns 0x6F on testnet (P2PKH testnet)" do
      Application.put_env(:athanor, :network, "testnet")
      start_network!()
      assert Network.address_version() == 0x6F
    end
  end
end
