defmodule Athanor.Fixtures do
  @moduledoc """
  Factory functions for creating test data across all schemas.

  Each function returns a valid attributes map. Use `*_fixture/1` variants
  to insert records directly into the database.
  """

  alias Athanor.Repo

  alias Athanor.Schema.{
    WatchingAddress,
    WatchingToken,
    MetaTransaction,
    Utxo,
    Broadcast,
    AddressHistory,
    BlockProcessContext
  }

  @doc "Returns valid attributes for a WatchingAddress."
  def watching_address_attrs(overrides \\ %{}) do
    Map.merge(
      %{address: "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", name: "test-address"},
      overrides
    )
  end

  @doc "Inserts a WatchingAddress into the database."
  def watching_address_fixture(overrides \\ %{}) do
    {:ok, addr} =
      %WatchingAddress{}
      |> WatchingAddress.changeset(watching_address_attrs(overrides))
      |> Repo.insert()

    addr
  end

  @doc "Returns valid attributes for a WatchingToken."
  def watching_token_attrs(overrides \\ %{}) do
    Map.merge(
      %{token_id: "abc123def456", symbol: "TST"},
      overrides
    )
  end

  @doc "Inserts a WatchingToken into the database."
  def watching_token_fixture(overrides \\ %{}) do
    {:ok, token} =
      %WatchingToken{}
      |> WatchingToken.changeset(watching_token_attrs(overrides))
      |> Repo.insert()

    token
  end

  @doc "Returns valid attributes for a MetaTransaction."
  def meta_transaction_attrs(overrides \\ %{}) do
    Map.merge(
      %{
        txid: :crypto.strong_rand_bytes(32),
        hex: "0100000001" <> String.duplicate("00", 50),
        timestamp: System.os_time(:second),
        is_confirmed: false,
        addresses: ["1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"],
        token_ids: [],
        metadata: %{}
      },
      overrides
    )
  end

  @doc "Inserts a MetaTransaction into the database."
  def meta_transaction_fixture(overrides \\ %{}) do
    {:ok, tx} =
      %MetaTransaction{}
      |> MetaTransaction.changeset(meta_transaction_attrs(overrides))
      |> Repo.insert()

    tx
  end

  @doc "Returns valid attributes for a Utxo."
  def utxo_attrs(overrides \\ %{}) do
    Map.merge(
      %{
        txid: :crypto.strong_rand_bytes(32),
        vout: 0,
        address: "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa",
        satoshis: 100_000,
        script_hex: "76a91462e907b15cbf27d5425399ebf6f0fb50ebb88f1888ac",
        is_spent: false
      },
      overrides
    )
  end

  @doc "Inserts a Utxo into the database."
  def utxo_fixture(overrides \\ %{}) do
    {:ok, utxo} =
      %Utxo{}
      |> Utxo.changeset(utxo_attrs(overrides))
      |> Repo.insert()

    utxo
  end

  @doc "Returns valid attributes for a Broadcast."
  def broadcast_attrs(overrides \\ %{}) do
    Map.merge(
      %{
        txid: "aabbccdd" <> String.duplicate("00", 28),
        hex: "0100000001" <> String.duplicate("00", 50),
        status: "pending"
      },
      overrides
    )
  end

  @doc "Inserts a Broadcast into the database."
  def broadcast_fixture(overrides \\ %{}) do
    {:ok, broadcast} =
      %Broadcast{}
      |> Broadcast.changeset(broadcast_attrs(overrides))
      |> Repo.insert()

    broadcast
  end

  @doc "Returns valid attributes for an AddressHistory entry."
  def address_history_attrs(overrides \\ %{}) do
    Map.merge(
      %{
        address: "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa",
        txid: "aabbccdd" <> String.duplicate("00", 28),
        direction: "in",
        satoshis: 50_000,
        timestamp: System.os_time(:second)
      },
      overrides
    )
  end

  @doc "Inserts an AddressHistory entry into the database."
  def address_history_fixture(overrides \\ %{}) do
    {:ok, history} =
      %AddressHistory{}
      |> AddressHistory.changeset(address_history_attrs(overrides))
      |> Repo.insert()

    history
  end

  @doc "Returns valid attributes for a BlockProcessContext."
  def block_process_context_attrs(overrides \\ %{}) do
    Map.merge(
      %{
        id: "000000000000000" <> String.duplicate("a", 49),
        height: 800_000,
        processed_at: DateTime.utc_now() |> DateTime.truncate(:second)
      },
      overrides
    )
  end

  @doc "Inserts a BlockProcessContext into the database."
  def block_process_context_fixture(overrides \\ %{}) do
    {:ok, ctx} =
      %BlockProcessContext{}
      |> BlockProcessContext.changeset(block_process_context_attrs(overrides))
      |> Repo.insert()

    ctx
  end
end
