defmodule Athanor.Indexer.Bootstrap do
  @moduledoc """
  Phase 7 F7.2 (T7.S) — the persisted **bootstrap boundary** for the thin indexer.

  The boundary is the lowest height the index anchors at; it is captured **once**
  (the node tip height at first start) and is stable across restarts. The no-gap
  predecessor guard (`BlockProcessor.predecessor_status/3`) accepts a block with a
  missing predecessor **only** when it is exactly this boundary block, so the
  contiguity invariant is total.
  """

  alias Athanor.Repo
  alias Athanor.Schema.IndexerBootstrap

  # The singleton row id.
  @id "bootstrap"

  @doc "The persisted boundary as `%{height, hash}`, or `nil` if not yet captured."
  @spec fetch() :: %{height: non_neg_integer(), hash: String.t() | nil} | nil
  def fetch do
    case Repo.get(IndexerBootstrap, @id) do
      %IndexerBootstrap{height: height, hash: hash} -> %{height: height, hash: hash}
      nil -> nil
    end
  end

  @doc """
  Captures the bootstrap boundary **once**. If already set, the existing boundary is
  returned unchanged (idempotent) — the anchor never moves after first capture.

  ## Parameters
    - `height` — the bootstrap block height (typically the node tip at first start).
    - `hash` — optional display-order block hash to pin the boundary.

  ## Returns
    The boundary as `%{height, hash}`.
  """
  @spec ensure(non_neg_integer(), String.t() | nil) ::
          %{height: non_neg_integer(), hash: String.t() | nil}
  def ensure(height, hash \\ nil) do
    case fetch() do
      nil ->
        %IndexerBootstrap{}
        |> IndexerBootstrap.changeset(%{id: @id, height: height, hash: hash})
        |> Repo.insert(on_conflict: :nothing)

        fetch()

      existing ->
        existing
    end
  end
end
