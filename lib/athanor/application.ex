defmodule Athanor.Application do
  @moduledoc """
  OTP Application module for Athanor.

  Starts the full supervision tree per PRD §5:
  - Core infrastructure (Repo, PubSub, Endpoint)
  - Blockchain interaction (Network → RPC → ZMQ)
  - Indexing pipeline (Filter, Processor, UtxoManager, BlockProcessor)
  - Background workers (monitors, verifiers, syncers)
  - Infrastructure (Finch HTTP pool, JungleBus client)
  - Registry for per-connection subscription tracking
  """

  use Application

  @impl true
  def start(_type, _args) do
    children =
      [
        AthanorWeb.Telemetry,
        Athanor.Repo,
        {DNSCluster, query: Application.get_env(:athanor, :dns_cluster_query) || :ignore},
        {Phoenix.PubSub, name: Athanor.PubSub},
        {Registry, keys: :duplicate, name: Athanor.Subscriptions}
      ] ++
        runtime_children() ++
        p2p_children() ++
        [
          # Phoenix endpoint — must be last
          AthanorWeb.Endpoint
        ]

    opts = [strategy: :one_for_one, name: Athanor.Supervisor]
    Supervisor.start_link(children, opts)
  end

  # In test mode, skip blockchain/indexer/worker/infra supervisors
  # to avoid ZMQ/RPC connections and DB access before sandbox is ready.
  defp runtime_children do
    if Application.get_env(:athanor, :skip_runtime_children, false) do
      []
    else
      [
        # Blockchain: network config → RPC → ZMQ (:rest_for_one)
        Athanor.Blockchain.Supervisor,

        # Indexer: filter, processor, UTXO manager, block processor (:one_for_one)
        Athanor.Indexer.Supervisor,

        # Workers: monitors, verifiers, syncers (:one_for_one)
        Athanor.Workers.Supervisor,

        # Infra: Finch HTTP pool, JungleBus client (:one_for_one)
        Athanor.Infra.Supervisor
      ]
    end
  end

  # The P2P peer pool runs as a config-gated sibling supervisor, off by default
  # (enable with `config :athanor, Athanor.P2P, enabled: true`).
  defp p2p_children do
    if Athanor.P2P.Supervisor.enabled?() do
      [{Athanor.P2P.Supervisor, []}]
    else
      []
    end
  end

  @impl true
  def config_change(changed, _new, removed) do
    AthanorWeb.Endpoint.config_change(changed, removed)
    :ok
  end
end
