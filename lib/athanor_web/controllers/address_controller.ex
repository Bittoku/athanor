defmodule AthanorWeb.AddressController do
  @moduledoc """
  Address endpoints for querying balances, history, and UTXOs.

  Delegates to service modules which query the indexed data in PostgreSQL.
  Service stubs return empty/zero results until the indexing pipeline is
  implemented in Phase 3.
  """

  use AthanorWeb, :controller

  alias Athanor.Services.{Balance, AddressHistory}
  alias Athanor.Indexer.UtxoManager

  @doc """
  GET /api/address/:address/balance — Returns BSV + token balances.

  ## Parameters
    - `address` — BSV address (path param)

  ## Responses
    - 200: Balance object with `address` and `bsv` (satoshis)
  """
  def balance(conn, %{"address" => address}) do
    bsv_balance = Balance.get_balance(address)
    json(conn, %{address: address, bsv: bsv_balance, tokens: []})
  end

  @doc """
  GET /api/address/:address/history — Returns paginated tx history.

  ## Parameters
    - `address` — BSV address (path param)
    - `skip` (query, optional) — number of records to skip (default 0)
    - `take` (query, optional) — number of records to return (default 50)

  ## Responses
    - 200: Object with `address` and `history` array
  """
  def history(conn, %{"address" => address} = params) do
    opts = [
      skip: parse_int(params["skip"], 0),
      take: parse_int(params["take"], 50)
    ]

    entries = AddressHistory.list(address, opts)

    json(conn, %{address: address, history: entries})
  end

  @doc """
  GET /api/address/:address/utxos — Returns unspent outputs.

  ## Parameters
    - `address` — BSV address (path param)

  ## Responses
    - 200: Object with `address` and `utxos` array
  """
  def utxos(conn, %{"address" => address}) do
    unspent = UtxoManager.list_unspent(address)

    utxo_list =
      Enum.map(unspent, fn u ->
        %{
          txid: Base.encode16(u.txid, case: :lower),
          vout: u.vout,
          satoshis: u.satoshis,
          script_hex: u.script_hex,
          token_id: u.token_id
        }
      end)

    json(conn, %{address: address, utxos: utxo_list})
  end

  # Parses a string to integer, returning default if nil or invalid.
  defp parse_int(nil, default), do: default

  defp parse_int(val, default) when is_binary(val) do
    case Integer.parse(val) do
      {n, _} -> n
      :error -> default
    end
  end

  defp parse_int(val, _default) when is_integer(val), do: val
end
