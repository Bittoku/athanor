defmodule Athanor.P2P.SourceRouter do
  @moduledoc """
  Phase 5 §A — the **pure, config-driven capability router**. It answers one
  question with no process, no IO, and no peer awareness:

      resolve(capability) :: {primary :: provider, fallbacks :: [provider]}

  "Which source serves capability X" becomes **config, not hardcoded**. A thin
  `route/2` runner (added in T5.4) turns the resolved tuple into an attempt
  sequence and is the only place `:p2p` is gated on live peers (`p2p_available?`),
  so the router itself never blocks on cold start.

  ## Capabilities & providers
    * capabilities — `:raw_tx_fetch | :broadcast | :realtime_ingest |
      :validation_fetch | :block_backfill | :historical_scan | :balance_utxo_fetch`.
    * providers — `:p2p | :rpc | :whatsonchain | :bitails | :junglebus | :zmq`.

  ## Default route table (the honesty contract)

  Only the capabilities a peer can actually serve are P2P-primary; everything a
  peer cannot authoritatively answer stays REST/RPC. `:realtime_ingest` lists
  `:p2p` first as a **provenance preference only** — it is a fan-in (ZMQ + SSE +
  P2P all publish, dedup downstream), never an exclusive cascade (see the broadcast
  /realtime integration, §C).

      capability          primary        fallbacks
      raw_tx_fetch        :p2p           [:rpc, :junglebus, :whatsonchain]
      broadcast           :p2p           [:rpc]
      realtime_ingest     :p2p           [:zmq, :junglebus]   (fan-in; not exclusive)
      validation_fetch    :rpc           []
      block_backfill      :rpc           [:junglebus]
      historical_scan     :whatsonchain  []
      balance_utxo_fetch  :whatsonchain  [:bitails]

  Override per capability with
  `config :athanor, Athanor.P2P.SourceRouter, routes: %{capability => {primary, fallbacks}}`;
  the override **deep-merges** (only the named capabilities change).
  """

  @type provider :: :p2p | :rpc | :whatsonchain | :bitails | :junglebus | :zmq
  @type capability ::
          :raw_tx_fetch
          | :broadcast
          | :realtime_ingest
          | :validation_fetch
          | :block_backfill
          | :historical_scan
          | :balance_utxo_fetch
  @type route :: {provider(), [provider()]}

  # The single source of truth for default routing — mirrors the moduledoc table.
  @defaults %{
    raw_tx_fetch: {:p2p, [:rpc, :junglebus, :whatsonchain]},
    broadcast: {:p2p, [:rpc]},
    realtime_ingest: {:p2p, [:zmq, :junglebus]},
    validation_fetch: {:rpc, []},
    block_backfill: {:rpc, [:junglebus]},
    historical_scan: {:whatsonchain, []},
    balance_utxo_fetch: {:whatsonchain, [:bitails]}
  }

  @doc """
  Resolves a capability to its `{primary, fallbacks}` route, deep-merging any
  `config :athanor, Athanor.P2P.SourceRouter, routes: …` override over the
  defaults.

  ## Parameters
    - `capability` — one of the known capability atoms (see the moduledoc).

  ## Returns
    `{primary_provider, [fallback_provider]}`.

  Raises `ArgumentError` for an unknown capability — routing a capability that
  isn't in the table is a programmer error, not a runtime fallthrough.
  """
  @spec resolve(capability()) :: route()
  def resolve(capability) do
    case Map.fetch(routes(), capability) do
      {:ok, route} -> route
      :error -> raise ArgumentError, "unknown routing capability: #{inspect(capability)}"
    end
  end

  @doc "The fully-resolved route table (defaults deep-merged with config overrides)."
  @spec routes() :: %{capability() => route()}
  def routes do
    overrides = Application.get_env(:athanor, __MODULE__, [])[:routes] || %{}
    Map.merge(@defaults, overrides)
  end

  @doc """
  Runs `attempt_fun` against the resolved providers for `capability`, in order
  (primary then each fallback), returning the **first** `{:ok, result}`. A
  provider that answers `:miss` (no data) or `{:error, reason}` advances to the
  next; if none succeed, returns the **last** `{:error, _}` seen, or `:miss` if
  every provider merely missed.

  A `:p2p` provider is **skipped as an instant miss** when `p2p_available?` is
  false (P2P disabled or zero live peers) — this is the single cold-start gate, so
  a P2P-primary route never blocks when there are no peers.

  ## Parameters
    - `capability` — the capability to resolve and route.
    - `attempt_fun` — `(provider -> {:ok, result} | :miss | {:error, reason})`.
    - `opts` — `:p2p_available?` (a boolean or 0-arity fun; default
      `Supervisor.enabled?/0 and PeerRegistry.pids/1 != []`).

  ## Returns
    `{:ok, result} | {:error, reason} | :miss`.
  """
  @spec route(capability(), (provider() -> {:ok, term()} | :miss | {:error, term()}), keyword()) ::
          {:ok, term()} | {:error, term()} | :miss
  def route(capability, attempt_fun, opts \\ []) when is_function(attempt_fun, 1) do
    {primary, fallbacks} = resolve(capability)
    do_route([primary | fallbacks], attempt_fun, opts, :miss)
  end

  defp do_route([], _attempt_fun, _opts, last), do: last

  defp do_route([provider | rest], attempt_fun, opts, last) do
    if provider == :p2p and not p2p_available?(opts) do
      do_route(rest, attempt_fun, opts, last)
    else
      case safe_attempt(attempt_fun, provider) do
        {:ok, _result} = ok -> ok
        :miss -> do_route(rest, attempt_fun, opts, last)
        {:error, _reason} = err -> do_route(rest, attempt_fun, opts, err)
      end
    end
  end

  # A provider that raises or exits (e.g. a `GenServer.call` to a down process, a
  # client timeout) must NOT crash the whole route — it degrades to the next
  # provider. Normalize any such failure to `{:error, _}` so routing continues.
  defp safe_attempt(attempt_fun, provider) do
    attempt_fun.(provider)
  rescue
    error -> {:error, {:provider_raised, provider, error}}
  catch
    :exit, reason -> {:error, {:provider_exited, provider, reason}}
    kind, reason -> {:error, {:provider_threw, provider, kind, reason}}
  end

  defp p2p_available?(opts) do
    case Keyword.fetch(opts, :p2p_available?) do
      {:ok, fun} when is_function(fun, 0) -> fun.()
      {:ok, bool} when is_boolean(bool) -> bool
      :error -> Athanor.P2P.Supervisor.enabled?() and Athanor.P2P.PeerRegistry.pids() != []
    end
  end
end
