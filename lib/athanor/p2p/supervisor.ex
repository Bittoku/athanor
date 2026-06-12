defmodule Athanor.P2P.Supervisor do
  @moduledoc """
  Supervises the P2P peer pool (Phase 2, T2.5).

  Owns two children under `:rest_for_one`:

    1. `Athanor.P2P.PeerRegistry` (the live-peer view), then
    2. `Athanor.P2P.PeerPool` (the dialer).

  `:rest_for_one` is deliberate: if the registry dies the pool must restart too
  (it holds registry state), but a pool crash leaves the registry intact.

  This supervisor is **config-gated and off by default** (per the plan, P2P is
  disabled until soak-tested). It runs as a *sibling* of the existing runtime
  supervisors — `Athanor.Application` adds it only when `enabled?/0` is true.
  Enable with:

      config :athanor, Athanor.P2P, enabled: true, network: :testnet, target: 8
  """

  use Supervisor

  alias Athanor.P2P.{Network, PeerPool, PeerRegistry}
  alias Athanor.P2P.Messages.Version

  # Application-env key for the P2P stack: `config :athanor, Athanor.P2P, ...`.
  @config_key Athanor.P2P

  @doc """
  Starts the supervisor. `opts` may carry `:pool_config` (a `PeerPool.Config`);
  if omitted, one is built from application env via `runtime_pool_config/0`.
  """
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @impl true
  def init(opts) do
    pool_config = Keyword.get(opts, :pool_config) || runtime_pool_config()

    children = [
      {PeerRegistry, [name: PeerRegistry]},
      {PeerPool, pool_config}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end

  @doc "Whether the P2P stack is enabled in application config (default false)."
  @spec enabled?() :: boolean()
  def enabled?, do: Application.get_env(:athanor, @config_key, [])[:enabled] == true

  @doc """
  Builds a `PeerPool.Config` from application env (`config :athanor, Athanor.P2P`).
  Reads `:network` (default `:testnet`) and `:target` (default 8); advertises a
  default `Athanor` version.
  """
  @spec runtime_pool_config() :: PeerPool.Config.t()
  def runtime_pool_config do
    env = Application.get_env(:athanor, @config_key, [])
    network = Network.for_network(Keyword.get(env, :network, :testnet))

    %PeerPool.Config{
      network: network,
      target: Keyword.get(env, :target, 8),
      our_version: default_version()
    }
  end

  defp default_version do
    na = Version.net_addr(0, <<0::128>>, 0)

    %Version{
      addr_recv: na,
      addr_from: na,
      nonce: :rand.uniform(0xFFFFFFFFFFFFFFFF),
      user_agent: "/Athanor:0.1.0/",
      start_height: 0
    }
  end
end
