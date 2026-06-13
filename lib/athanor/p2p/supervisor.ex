defmodule Athanor.P2P.Supervisor do
  @moduledoc """
  Supervises the P2P peer pool (Phase 2, T2.5).

  Owns five children under `:rest_for_one`:

    1. `Athanor.P2P.PeerRegistry` (the live-peer view), then
    2. `Athanor.P2P.MempoolObserver` (Phase 3 §C — inbound mempool ingest), then
    3. `Athanor.P2P.TxRelay` (Phase 4 §A — outbound broadcast + relay-back), then
    4. `Athanor.P2P.TxFetcher` (Phase 5 §B — `getdata` pull-fetch by txid), then
    5. `Athanor.P2P.PeerPool` (the dialer), with `frame_sink` set to the
       **fan-out list** `[MempoolObserver, TxRelay, TxFetcher]` so each forwarded
       frame reaches every consumer (the observer cares about `inv`/`tx`/
       `notfound`; the relay about `getdata`/`inv`/`reject`; the fetcher about
       `tx`/`notfound` for txids it is actively pulling).

  `:rest_for_one` is deliberate and ordered: every sink name must be registered
  before the pool forwards frames to them; a registry restart cascades to all;
  a pool crash leaves the registry, observer, relay, and fetcher intact.

  This supervisor is **config-gated and off by default** (per the plan, P2P is
  disabled until soak-tested). It runs as a *sibling* of the existing runtime
  supervisors — `Athanor.Application` adds it only when `enabled?/0` is true.
  Enable with:

      config :athanor, Athanor.P2P, enabled: true, network: :testnet, target: 8
  """

  use Supervisor
  require Logger

  alias Athanor.P2P.{
    MempoolObserver,
    Network,
    PeerPool,
    PeerRegistry,
    TxFetcher,
    TxRelay,
    Watchlist
  }

  alias Athanor.P2P.Messages.Version
  alias Athanor.Services.Broadcast

  # The pool's frame_sink fan-out (§A): every post-handshake frame reaches the
  # inbound observer, the outbound relay, and the pull-fetcher (Phase 5).
  @frame_sinks [MempoolObserver, TxRelay, TxFetcher]

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

    # The pool fans each frame out to the [observer, relay] list (§A). Default
    # the sink to the registered sink names (so the wiring survives a child
    # restart), while honouring an explicitly-supplied sink.
    pool_config = %{pool_config | frame_sink: pool_config.frame_sink || @frame_sinks}

    observer_opts =
      opts
      |> Keyword.get(:observer_opts, default_observer_opts())
      |> Keyword.put_new(:name, MempoolObserver)

    relay_opts =
      opts
      |> Keyword.get(:relay_opts, default_relay_opts())
      |> Keyword.put_new(:name, TxRelay)

    fetcher_opts =
      opts
      |> Keyword.get(:fetcher_opts, [])
      |> Keyword.put_new(:name, TxFetcher)

    # `:rest_for_one`, ordered Registry → Observer → TxRelay → TxFetcher → Pool:
    # every sink name must be registered before the pool starts forwarding frames
    # to them, and a registry restart cascades to all.
    children = [
      {PeerRegistry, [name: PeerRegistry]},
      {MempoolObserver, observer_opts},
      {TxRelay, relay_opts},
      {TxFetcher, fetcher_opts},
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
      frame_sink: @frame_sinks
    }
  end

  # Observer options for the supervised child. The prefilter watchlist is seeded
  # from the persisted `WatchingAddress` set; the matcher (`matches?/1`) and the
  # indexing pipeline use their production defaults.
  defp default_observer_opts, do: [watchlist: build_watchlist()]

  # Relay options for the supervised child. The audit sink bridges relay
  # lifecycle events back to the `broadcasts` audit rows (§C); target selection
  # and TTL/cap use their production defaults.
  defp default_relay_opts, do: [audit: &Broadcast.apply_relay_event/1]

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
    catch
      # A DB-pool checkout failure surfaces as an `exit`, not a raise (e.g. the
      # connection owner went away). The supervisor must still start with an empty
      # prefilter rather than crash — same defensive intent as the rescue above.
      :exit, reason ->
        Logger.debug("P2P watchlist seed skipped (exit #{inspect(reason)}); starting empty")
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
