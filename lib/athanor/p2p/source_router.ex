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
end
