defmodule Athanor.Infra.Bitails do
  @moduledoc """
  REST client for the Bitails API.
  Provides additional transaction data not available from the BSV node.

  HTTP transport options are read from `Application.get_env(:athanor,
  :bitails_http_opts, [finch: Athanor.Finch])`. Tests set
  `[plug: {Req.Test, :bitails_stub}]` to route through a `Req.Test` stub.
  """

  require Logger

  @base_url "https://api.bitails.io"

  @doc """
  Fetches transaction details from Bitails.
  """
  def get_tx(txid) do
    url = "#{@base_url}/tx/#{txid}"

    case Req.get(url, http_opts()) do
      {:ok, %Req.Response{status: 200, body: body}} when is_map(body) ->
        {:ok, body}

      {:ok, %Req.Response{status: status}} ->
        {:error, {:http_error, status}}

      {:error, reason} ->
        Logger.warning("Bitails get_tx failed: #{inspect(reason)}")
        {:error, reason}
    end
  end

  @doc """
  Fetches raw transaction hex from Bitails.
  """
  def get_raw_tx(txid) do
    url = "#{@base_url}/tx/#{txid}/raw"

    case Req.get(url, http_opts()) do
      {:ok, %Req.Response{status: 200, body: body}} when is_binary(body) ->
        {:ok, String.trim(body)}

      {:ok, %Req.Response{status: status}} ->
        {:error, {:http_error, status}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Fetches address balance from Bitails.
  """
  def get_address_balance(address) do
    url = "#{@base_url}/address/#{address}/balance"

    case Req.get(url, http_opts()) do
      {:ok, %Req.Response{status: 200, body: body}} when is_map(body) ->
        {:ok, body}

      {:ok, %Req.Response{status: status}} ->
        {:error, {:http_error, status}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp http_opts, do: Application.get_env(:athanor, :bitails_http_opts, finch: Athanor.Finch)
end
