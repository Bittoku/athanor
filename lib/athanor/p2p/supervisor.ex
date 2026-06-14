defmodule Athanor.P2P.Supervisor do
  @moduledoc """
  Supervises the P2P peer pool (Phase 2, T2.5).

  Owns six children under `:rest_for_one`:

    1. `Athanor.P2P.PeerRegistry` (the live-peer view), then
    2. `Athanor.P2P.MempoolObserver` (Phase 3 §C — inbound mempool ingest), then
    3. `Athanor.P2P.TxRelay` (Phase 4 §A — outbound broadcast + relay-back), then
    4. `Athanor.P2P.TxFetcher` (Phase 5 §B — `getdata` pull-fetch by txid), then
    5. `Athanor.P2P.HeadersChain` (Phase 6 §B — headers chain + reorg detection), then
    6. `Athanor.P2P.PeerPool` (the dialer), with `frame_sink` set to the
       **fan-out list** `[MempoolObserver, TxRelay, TxFetcher, HeadersChain]` so each
       forwarded frame reaches every consumer (the observer cares about `inv`/`tx`/
       `notfound`; the relay about `getdata`/`inv`/`reject`; the fetcher about
       `tx`/`notfound`; the headers chain about `inv(MSG_BLOCK)`/`headers`).

  `:rest_for_one` is deliberate and ordered: every sink name must be registered
  before the pool forwards frames to them; a registry restart cascades to all;
  a pool crash leaves the registry, observer, relay, fetcher, and headers chain intact.

  This supervisor is **config-gated and off by default** (per the plan, P2P is
  disabled until soak-tested). It runs as a *sibling* of the existing runtime
  supervisors — `Athanor.Application` adds it only when `enabled?/0` is true.
  Enable with:

      config :athanor, Athanor.P2P, enabled: true, network: :testnet, target: 8
  """

  use Supervisor
  require Logger

  alias Athanor.P2P.{
    HeadersChain,
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
  # inbound observer, the outbound relay, the pull-fetcher (Phase 5), and the
  # headers chain (Phase 6).
  @frame_sinks [MempoolObserver, TxRelay, TxFetcher, HeadersChain]

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

    headers_opts =
      opts
      |> Keyword.get(:headers_opts, default_headers_opts(pool_config.network))
      |> Keyword.put_new(:name, HeadersChain)

    # `:rest_for_one`, ordered Registry → Observer → TxRelay → TxFetcher →
    # HeadersChain → Pool: every sink name must be registered before the pool
    # starts forwarding frames to them, and a registry restart cascades to all.
    children = [
      {PeerRegistry, [name: PeerRegistry]},
      {MempoolObserver, observer_opts},
      {TxRelay, relay_opts},
      {TxFetcher, fetcher_opts},
      {HeadersChain, headers_opts},
      {PeerPool, pool_config}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end

  @doc "Whether the P2P stack is enabled in application config (default false)."
  @spec enabled?() :: boolean()
  def enabled?, do: Application.get_env(:athanor, @config_key, [])[:enabled] == true

  @doc """
  Builds a `PeerPool.Config` from application env (`config :athanor, Athanor.P2P`).
  Reads `:target` (default 8) and `:network`; advertises a default `Athanor` version.

  The P2P network — and therefore the armed cw-144 DAA gate (mainnet checker vs the
  testnet fail-closed stub) — defaults to the **authoritative application network**
  (`Athanor.Blockchain.Network.resolve/0`, the pure config reader for
  `config :athanor, :network`), so an operator setting `NETWORK=mainnet` reliably
  arms mainnet validation without a second `Athanor.P2P` network key. An explicit
  `config :athanor, Athanor.P2P, network:` override still wins (resolved lazily so it
  is honoured without consulting the application network).
  """
  @spec runtime_pool_config() :: PeerPool.Config.t()
  def runtime_pool_config do
    env = Application.get_env(:athanor, @config_key, [])

    network =
      Network.for_network(
        Keyword.get_lazy(env, :network, fn -> Athanor.Blockchain.Network.resolve() end)
      )

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

  # HeadersChain options for the supervised child. The synthetic root is seeded from
  # the node's current best block over RPC, and the header PoW gate is bound to the
  # active network's consensus pow-limit. Window/tick use their defaults.
  #
  # Phase 7 F7.2: the chain's tip events are an advisory **hint** to the single
  # index-tip mutation owner (`Athanor.Indexer.TipController.notify_tip/1`). The
  # chain's low-latency reorg detection thus *accelerates* the RPC-confirmed
  # reconcile without granting P2P any index-mutation authority.
  defp default_headers_opts(network) do
    [
      seed: &rpc_seed/0,
      on_tip: &Athanor.Indexer.TipController.notify_tip/1,
      pow_limit: network.pow_limit,
      network: network.name
    ]
  end

  # The cw-144 DAA needs the parent and its 146 ancestors; F7.1 §D1 seeds that full
  # window below the checkpoint so the first P2P header is DAA-validated.
  @daa_seed_window 147

  # Seed the header tree from the node's current best block via RPC, **with the full
  # cw-144 bootstrap window** (F7.1 §D1). Walks `getblockheader` from the tip down
  # `@daa_seed_window` blocks via each header's `prev_block`, and returns a
  # window-capable seed `{:ok, root_height, root_wire_hash, [%BlockHeader{} ascending]}`
  # — the synthetic root sits at `tip - 147` and the 147 real headers are planted
  # above it. Any RPC/decoding failure returns `{:error, _}`; the chain then starts
  # inert and retries on tick (a failed seed never crashes the supervisor, and it
  # must not seed without a window under the armed DAA gate).
  defp rpc_seed do
    with {:ok, tip_height} <- Athanor.Blockchain.RpcClient.get_block_count(),
         {:ok, tip_hash_hex} <- Athanor.Blockchain.RpcClient.get_block_hash(tip_height),
         {:ok, ascending} <- fetch_header_window(tip_hash_hex, @daa_seed_window) do
      first = hd(ascending)
      root_wire_hash = Athanor.P2P.Messages.BlockHeader.prev_hash_wire(first)
      {:ok, tip_height - @daa_seed_window, root_wire_hash, ascending}
    else
      other -> {:error, other}
    end
  end

  # Walk `count` headers down from `start_hash_hex` (display hex) via each header's
  # `prev_block`, returning them **ascending** (lowest height first) as
  # `%BlockHeader{}`, or `{:error, _}` on any RPC/decode failure.
  defp fetch_header_window(start_hash_hex, count) do
    1..count
    |> Enum.reduce_while({:ok, [], start_hash_hex}, fn _i, {:ok, acc, hash_hex} ->
      case fetch_header(hash_hex) do
        {:ok, header} ->
          parent_display = Athanor.P2P.Messages.BlockHeader.prev_hash(header)
          {:cont, {:ok, [header | acc], Base.encode16(parent_display, case: :lower)}}

        err ->
          {:halt, err}
      end
    end)
    |> case do
      {:ok, headers, _next} -> {:ok, headers}
      err -> err
    end
  end

  # The 80-byte header for `hash_hex` as a `%BlockHeader{}` (via `getblockheader`,
  # which is tiny even for very large blocks).
  defp fetch_header(hash_hex) do
    with {:ok, header_hex} <- Athanor.Blockchain.RpcClient.get_block_header(hash_hex, false),
         {:ok, <<raw::binary-80, _::binary>>} <- Base.decode16(header_hex, case: :mixed) do
      {:ok, %Athanor.P2P.Messages.BlockHeader{raw: raw}}
    else
      other -> {:error, other}
    end
  end

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
