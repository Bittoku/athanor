defmodule Athanor.Indexer.UtxoManager do
  @moduledoc """
  Manages the UTXO set in PostgreSQL. Provides functions to create UTXOs,
  mark them as spent, and query unspent outputs by address or token.

  This is a plain module (not a GenServer) — UTXO operations are stateless
  database queries that can be called from any process.
  """

  alias Athanor.Repo
  alias Athanor.Schema.Utxo
  import Ecto.Query

  @doc """
  Returns all unspent UTXOs for a given address.
  """
  def list_unspent(address) do
    Utxo
    |> where([u], u.address == ^address and u.is_spent == false)
    |> Repo.all()
  end

  @doc """
  Returns all unspent token UTXOs for a given token ID and optional address.
  """
  def list_token_utxos(token_id, address \\ nil) do
    query = Utxo |> where([u], u.token_id == ^token_id and u.is_spent == false)

    query =
      if address do
        where(query, [u], u.address == ^address)
      else
        query
      end

    Repo.all(query)
  end

  @doc """
  Creates a new UTXO record. Uses on_conflict: :nothing to handle duplicates.
  """
  def create_utxo(attrs) do
    %Utxo{}
    |> Utxo.changeset(attrs)
    |> Repo.insert(on_conflict: :nothing)
  end

  @doc """
  Marks a UTXO as spent by the given transaction.
  """
  def spend_utxo(txid, vout, spent_txid) do
    case Repo.get_by(Utxo, txid: txid, vout: vout) do
      nil ->
        {:error, :not_found}

      utxo ->
        utxo
        |> Utxo.changeset(%{is_spent: true, spent_txid: spent_txid})
        |> Repo.update()
    end
  end

  @doc """
  Records the STAS 3.0 operation class (`stas3_op`) for a previously
  indexed UTXO. The class is the spendType byte from the unlocking
  script of the input that consumed this UTXO (spec v0.1 §8.2 / §9.6).
  Silently ignored if the UTXO is unknown — non-watched STAS 3.0 inputs
  may legitimately reference outputs we never indexed.
  """
  def set_stas3_op(txid, vout, op) do
    case Repo.get_by(Utxo, txid: txid, vout: vout) do
      nil ->
        {:error, :not_found}

      utxo ->
        utxo
        |> Utxo.changeset(%{stas3_op: op})
        |> Repo.update()
    end
  end

  @doc """
  Unconfirm UTXOs at or above a given block height (for reorg rollback).
  """
  def unconfirm_above(height) do
    Utxo
    |> where([u], u.block_height > ^height)
    |> Repo.update_all(set: [block_height: nil])
  end

  @doc """
  Returns UTXO count and total satoshis for an address.
  """
  def stats(address) do
    result =
      Utxo
      |> where([u], u.address == ^address and u.is_spent == false)
      |> select([u], %{count: count(u.id), total: sum(u.satoshis)})
      |> Repo.one()

    %{count: result.count, total_satoshis: normalize_sum(result.total)}
  end

  defp normalize_sum(nil), do: 0
  defp normalize_sum(%Decimal{} = d), do: Decimal.to_integer(d)
  defp normalize_sum(n) when is_integer(n), do: n
end
