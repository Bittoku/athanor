defmodule Athanor.P2P.Supervisor do
  @moduledoc """
  Supervises the P2P peer pool (Phase 2, T2.5).

  Owns three children under `:rest_for_one`:

    1. `Athanor.P2P.PeerRegistry` (the live-peer view), then
    2. `Athanor.P2P.MempoolObserver` (Phase 3 §C — the pool's `frame_sink`), then
    3. `Athanor.P2P.PeerPool` (the dialer), with `frame_sink` set to the
       observer's registered name so forwarded frames reach it.

  `:rest_for_one` is deliberate and ordered: the observer's name must be
  registered before the pool forwards frames to it; a registry restart cascades
  to both; a pool crash leaves the registry and observer intact.

  This supervisor is **config-gated and off by default** (per the plan, P2P is
  disabled until soak-tested). It runs as a *sibling* of the existing runtime
  supervisors — `Athanor.Application` adds it only when `enabled?/0` is true.
  Enable with:

      config :athanor, Athanor.P2P, enabled: true, network: :testnet, target: 8
  """

  use Supervisor
  require Logger

  alias Athanor.P2P.{MempoolObserver, Network, PeerPool, PeerRegistry, Watchlist}
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

    # The observer is the pool's frame sink (§C). Default the sink to the
    # observer's registered name (so the wiring survives an observer restart),
    # while honouring an explicitly-supplied sink.
    pool_config = %{pool_config | frame_sink: pool_config.frame_sink || MempoolObserver}

    observer_opts =
      opts
      |> Keyword.get(:observer_opts, default_observer_opts())
      |> Keyword.put_new(:name, MempoolObserver)

    # `:rest_for_one`, ordered Registry → Observer → Pool: the observer's name
    # must be registered before the pool starts forwarding frames to it, and a
    # registry restart cascades to both.
    children = [
      {PeerRegistry, [name: PeerRegistry]},
      {MempoolObserver, observer_opts},
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
      our_version: default_version(),
      frame_sink: MempoolObserver
    }
  end

  # Observer options for the supervised child. The prefilter watchlist is seeded
  # from the persisted `WatchingAddress` set; the matcher (`matches?/1`) and the
  # indexing pipeline use their production defaults.
  defp default_observer_opts, do: [watchlist: build_watchlist()]

  # Seed a `Watchlist` from the watched-address table. Wrapped defensively: a
  # transient DB unavailability at P2P start (P2P is enabled post-boot) must not
  # crash the supervisor — it starts with an empty prefilter and is repopulated
  # on the next restart rather than taking the tree down.
  defp build_watchlist do
    table = Watchlist.new()

    try do
      Athanor.Repo.all(Athanor.Schema.WatchingAddress)
      |> Enum.each(fn %{address: address} -> Watchlist.put_address(table, address) end)
    rescue
      error ->
        Logger.debug("P2P watchlist seed skipped (#{inspect(error)}); starting empty")
    end

    table
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
