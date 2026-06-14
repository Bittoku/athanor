defmodule Athanor.Infra.WhatsOnChain do
  @moduledoc """
  REST client for the WhatsOnChain (WoC) API.
  Used as a fallback for transaction lookups and address history.

  HTTP transport options are read from `Application.get_env(:athanor,
  :woc_http_opts, [finch: Athanor.Finch])`. Tests set
  `[plug: {Req.Test, :woc_stub}]` to route through a `Req.Test` stub.
  """

  require Logger

  @base_url "https://api.whatsonchain.com/v1/bsv"

  @doc """
  Fetches a raw transaction hex by txid.
  """
  def get_raw_tx(txid) do
    network = get_network()
    url = "#{@base_url}/#{network}/tx/#{txid}/hex"

    case Req.get(url, http_opts()) do
      {:ok, %Req.Response{status: 200, body: body}} when is_binary(body) ->
        {:ok, String.trim(body)}

      {:ok, %Req.Response{status: status}} ->
        {:error, {:http_error, status}}

      {:error, reason} ->
        Logger.warning("WoC get_raw_tx failed: #{inspect(reason)}")
        {:error, reason}
    end
  end

  @doc """
  Fetches transaction details (verbose) by txid.
  """
  def get_tx(txid) do
    network = get_network()
    url = "#{@base_url}/#{network}/tx/hash/#{txid}"

    case Req.get(url, http_opts()) do
      {:ok, %Req.Response{status: 200, body: body}} when is_map(body) ->
        {:ok, body}

      {:ok, %Req.Response{status: status}} ->
        {:error, {:http_error, status}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Fetches unspent outputs for an address.
  """
  def get_address_utxos(address) do
    network = get_network()
    url = "#{@base_url}/#{network}/address/#{address}/unspent"

    case Req.get(url, http_opts()) do
      {:ok, %Req.Response{status: 200, body: body}} when is_list(body) ->
        {:ok, body}

      {:ok, %Req.Response{status: status}} ->
        {:error, {:http_error, status}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Fetches transaction history (txid list) for an address.
  """
  def get_address_history(address) do
    network = get_network()
    url = "#{@base_url}/#{network}/address/#{address}/history"

    case Req.get(url, http_opts()) do
      {:ok, %Req.Response{status: 200, body: body}} when is_list(body) ->
        txids = Enum.map(body, fn entry -> entry["tx_hash"] end) |> Enum.reject(&is_nil/1)
        {:ok, txids}

      {:ok, %Req.Response{status: status}} ->
        {:error, {:http_error, status}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp get_network do
    case Application.get_env(:athanor, :network, "mainnet") do
      "testnet" -> "test"
      _ -> "main"
    end
  end

  defp http_opts, do: Application.get_env(:athanor, :woc_http_opts, finch: Athanor.Finch)
end
