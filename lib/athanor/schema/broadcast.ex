defmodule Athanor.Schema.Broadcast do
  @moduledoc """
  Records transaction broadcast attempts and their outcomes.

  ## Status lattice (Phase 4 §C)

  `status` is a single plain-string column (no PG enum, so no migration) holding
  one of the **monotonic, never-downgrading** tiers:

      rank | status      | set by
      -----+-------------+----------------------------------------------------
       0   | pending     | row created
       1   | relayed     | announced to ≥1 peer via P2P
       2   | unconfirmed | TTL tick with no propagation
       3   | rejected    | a node RPC error OR a P2P peer `reject`
       4   | accepted    | the RPC/REST broadcaster returned ok (single node)
       5   | propagated  | ≥bar distinct non-target peers re-announced it

  `advance_status/2` sets `status` to the higher-ranked of the current and the
  incoming value, never the lower — so `rejected` (3) overwrites an
  `unconfirmed` (2) row, but a late TTL `:unconfirmed` never overwrites an
  existing `rejected`/`accepted`/`propagated`. Cold-start (no P2P) collapses to
  exactly `pending → accepted | rejected`. On rejection, `error` carries the
  node/peer error message.
  """

  use Ecto.Schema
  import Ecto.Changeset

  @primary_key {:id, :binary_id, autogenerate: true}
  @foreign_key_type :binary_id

  schema "broadcasts" do
    field :txid, :string
    field :hex, :string
    field :status, :string, default: "pending"
    field :error, :string

    timestamps(type: :utc_datetime, updated_at: false)
  end

  # Ordered by rank (index = tier). The list order *is* the lattice.
  @status_rank ~w(pending relayed unconfirmed rejected accepted propagated)
  @valid_statuses @status_rank

  @doc """
  Builds a changeset for recording a broadcast attempt.

  ## Parameters
    - `broadcast` — existing struct or empty schema
    - `attrs` — map with :txid, :hex, :status (enum), and optional :error

  ## Returns
    An `Ecto.Changeset` validating status is one of
    pending/relayed/unconfirmed/rejected/accepted/propagated.
  """
  def changeset(broadcast, attrs) do
    broadcast
    |> cast(attrs, [:txid, :hex, :status, :error])
    |> validate_required([:txid, :hex, :status])
    |> validate_inclusion(:status, @valid_statuses)
  end

  @doc """
  Resolves two status values to the higher-ranked one (the monotonic,
  never-downgrading lattice — see the module doc).

  ## Parameters
    - `current` — the status already on the row.
    - `incoming` — the status an event wants to apply.

  ## Returns
    `incoming` if its rank is `>=` the current rank, else `current` (so an
    out-of-band event can never lower the tier). Unknown statuses rank as 0.
  """
  @spec advance_status(String.t(), String.t()) :: String.t()
  def advance_status(current, incoming) do
    if rank(incoming) >= rank(current), do: incoming, else: current
  end

  defp rank(status) do
    case Enum.find_index(@status_rank, &(&1 == status)) do
      nil -> 0
      index -> index
    end
  end
end
