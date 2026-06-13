defmodule Athanor.Indexer.Supervisor do
  @moduledoc """
  Supervises core indexing processes with :one_for_one strategy.

  Each indexer component is independent — one crash doesn't cascade.
  """

  use Supervisor

  @doc """
  Starts the indexer supervisor.
  """
  def start_link(init_arg) do
    Supervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  @impl true
  def init(_init_arg) do
    # `BlockProcessor` (the apply primitive) starts before `TipController` (the
    # single index-tip mutation owner + RPC reconcile driver) so the controller can
    # call `apply_branch/2` as soon as it ticks (Phase 7 F7.2 T7.S).
    children = [
      Athanor.Indexer.TransactionFilter,
      Athanor.Indexer.TransactionProcessor,
      Athanor.Indexer.BlockProcessor,
      Athanor.Indexer.TipController
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
