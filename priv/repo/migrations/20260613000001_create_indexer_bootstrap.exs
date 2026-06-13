defmodule Athanor.Repo.Migrations.CreateIndexerBootstrap do
  use Ecto.Migration

  def change do
    create table(:indexer_bootstrap, primary_key: false) do
      add :id, :string, primary_key: true
      add :height, :integer, null: false
      add :hash, :string
      timestamps()
    end
  end
end
