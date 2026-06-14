defmodule Athanor.Repo.Migrations.CreateAddressHistories do
  @moduledoc """
  Creates the address_histories table for per-address transaction history.
  Composite index on (address, timestamp DESC) for paginated queries.
  """

  use Ecto.Migration

  def change do
    create table(:address_histories, primary_key: false) do
      add :id, :binary_id, primary_key: true
      add :address, :text, null: false
      add :txid, :text, null: false
      add :direction, :text, null: false
      add :satoshis, :bigint, null: false
      add :token_id, :text
      add :block_height, :integer
      add :timestamp, :bigint, null: false

      timestamps(type: :utc_datetime, updated_at: false)
    end

    create index(:address_histories, ["address", "timestamp DESC"],
             name: :address_histories_address_timestamp_index
           )
  end
end
