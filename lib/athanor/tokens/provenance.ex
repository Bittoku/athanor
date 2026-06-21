defmodule Athanor.Tokens.Provenance do
  @moduledoc """
  Token lineage verification — confirms a STAS token traces back to a
  valid genesis issuance via the B2G resolver and local DB records.

  This is the indexer-trust (non-cryptographic) provenance model: a naive
  full back-trace walk over the input chain, NOT a trust-minimized proof.
  Correctness relies on the discoverable, open-sourced indexer set, not on
  any on-chain or cryptographic provenance guarantee.
  """

  alias Athanor.Repo
  alias Athanor.Schema.Utxo
  alias Athanor.Indexer.B2gResolver
  import Ecto.Query

  @doc """
  Verifies that a token output has valid provenance back to genesis.

  ## Parameters
    - `txid` — transaction ID (binary)
    - `vout` — output index

  ## Returns
    `{:ok, :valid}` | `{:error, reason}`
  """
  def verify(txid, vout) do
    case B2gResolver.resolve(txid, vout) do
      {:ok, chain} -> {:ok, %{valid: true, depth: length(chain), chain: chain}}
      {:error, _} = err -> err
    end
  end

  @doc """
  Returns the token lineage for display — list of txids from current back to genesis.
  """
  def lineage(txid, vout) do
    case B2gResolver.resolve(txid, vout) do
      {:ok, chain} -> {:ok, chain}
      {:error, _} = err -> err
    end
  end

  @doc """
  Gets token stats: total supply (unspent), burned (spent with no matching output).
  """
  def token_stats(token_id) do
    unspent_count =
      Utxo
      |> where([u], u.token_id == ^token_id and u.is_spent == false)
      |> select([u], count(u.id))
      |> Repo.one()

    total_satoshis =
      case Utxo
           |> where([u], u.token_id == ^token_id and u.is_spent == false)
           |> select([u], sum(u.satoshis))
           |> Repo.one() do
        nil -> 0
        %Decimal{} = d -> Decimal.to_integer(d)
        n when is_integer(n) -> n
      end

    spent_count =
      Utxo
      |> where([u], u.token_id == ^token_id and u.is_spent == true)
      |> select([u], count(u.id))
      |> Repo.one()

    %{
      token_id: token_id,
      unspent_count: unspent_count,
      total_satoshis: total_satoshis,
      spent_count: spent_count
    }
  end
end
