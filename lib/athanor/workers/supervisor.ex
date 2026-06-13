defmodule Athanor.Workers.Supervisor do
  @moduledoc """
  Supervises background worker processes with :one_for_one strategy.

  Each worker is independent — monitors, verifiers, and syncers can
  crash and restart without affecting each other.
  """

  use Supervisor

  @doc """
  Starts the workers supervisor.
  """
  def start_link(init_arg) do
    Supervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  @impl true
  def init(_init_arg) do
    # `Athanor.Workers.ChainTipVerifier` (the legacy RPC tip poller that cast
    # straight into `BlockProcessor`) is retired in Phase 7 F7.2: its RPC poll +
    # catch-up role is the `Athanor.Indexer.TipController` reconcile cycle — the
    # single index-tip mutation owner — so it is no longer supervised.
    children = [
      Athanor.Workers.UnconfirmedMonitor,
      Athanor.Workers.StasObserver,
      Athanor.Workers.MissingTxSyncer
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
