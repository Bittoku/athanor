defmodule Athanor.Schema.IndexerBootstrap do
  @moduledoc """
  The thin indexer's **bootstrap boundary** (Phase 7 F7.2 T7.S): the single
  (singleton) lowest height the index is anchored at. Captured once at first start
  (the node tip height) and stable across restarts, it is the only height at which
  the no-gap predecessor guard accepts a block with no recorded predecessor —
  making the contiguity invariant total.

  Primary key `id` is a fixed singleton string. `height` is the bootstrap block's
  height; `hash` (optional) pins its display-order block hash.
  """

  use Ecto.Schema
  import Ecto.Changeset

  @primary_key {:id, :string, autogenerate: false}

  schema "indexer_bootstrap" do
    field :height, :integer
    field :hash, :string
    timestamps()
  end

  @doc """
  Builds a changeset for the singleton bootstrap row.

  ## Parameters
    - `bootstrap` — existing struct or empty schema
    - `attrs` — map with `:id` (singleton), `:height`, and optional `:hash`

  ## Returns
    An `Ecto.Changeset` requiring `:id` and `:height`.
  """
  def changeset(bootstrap, attrs) do
    bootstrap
    |> cast(attrs, [:id, :height, :hash])
    |> validate_required([:id, :height])
  end
end
