defmodule Athanor.Blockchain.Network do
  @moduledoc """
  Holds network configuration (mainnet/testnet) and exposes helpers.
  """

  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Returns the current network (:mainnet or :testnet)."
  def network do
    GenServer.call(__MODULE__, :network)
  end

  @doc """
  Resolves the configured network (`:mainnet | :testnet`) **purely** from
  `config :athanor, :network` (default `"mainnet"`), without consulting the
  GenServer. This is the single source of truth `init/1` caches; callers that run
  before/without the process (e.g. the P2P supervisor building its pool config) use
  it directly so they cannot diverge from the authoritative network.
  """
  @spec resolve() :: :mainnet | :testnet
  def resolve do
    case Application.get_env(:athanor, :network, "mainnet") do
      "testnet" -> :testnet
      "stn" -> :testnet
      _ -> :mainnet
    end
  end

  @doc "Returns true if running on mainnet."
  def is_mainnet? do
    network() == :mainnet
  end

  @doc "Returns true if running on testnet."
  def is_testnet? do
    network() == :testnet
  end

  @doc "Returns the address version byte for the current network."
  def address_version do
    if is_mainnet?(), do: 0x00, else: 0x6F
  end

  ## ── Server ──

  @impl true
  def init(_opts) do
    {:ok, %{network: resolve()}}
  end

  @impl true
  def handle_call(:network, _from, state) do
    {:reply, state.network, state}
  end
end
