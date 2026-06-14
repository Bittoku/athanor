defmodule Athanor.Blockchain.RpcClient do
  @moduledoc """
  JSON-RPC client for communicating with the BSV node (bitcoind).

  GenServer that holds connection config (rpc_url, user, password) and
  provides public functions for common RPC methods. All calls are
  serialized through GenServer.call to ensure orderly request handling.

  Uses Req for HTTP POST with basic auth and JSON-RPC 2.0 envelope.
  """

  use GenServer
  require Logger

  ## ── Client API ──

  @doc """
  Starts the RPC client GenServer. Reads BSV node config from app env.

  ## Parameters
    - `opts` — keyword options passed to GenServer.start_link

  ## Returns
    `{:ok, pid}` on success.
  """
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Returns the current block height from the BSV node.

  ## Returns
    - `{:ok, height}` — integer block height
    - `{:error, reason}` — on RPC or network failure
  """
  def get_block_count do
    GenServer.call(__MODULE__, :get_block_count)
  end

  @doc """
  Returns the block hash for a given height.

  ## Parameters
    - `height` — integer block height

  ## Returns
    - `{:ok, hash_hex}` — 64-character hex string of the block hash
    - `{:error, reason}` — on RPC or network failure
  """
  def get_block_hash(height) do
    GenServer.call(__MODULE__, {:get_block_hash, height})
  end

  @doc """
  Returns block data for a given block hash.

  ## Parameters
    - `hash_hex` — 64-character hex string of the block hash
    - `verbosity` — 0 (hex string), 1 (JSON object), 2 (JSON with full tx data). Defaults to 1.

  ## Returns
    - `{:ok, block_map}` — map of block data (contents depend on verbosity)
    - `{:error, reason}` — on RPC or network failure
  """
  def get_block(hash_hex, verbosity \\ 1) do
    GenServer.call(__MODULE__, {:get_block, hash_hex, verbosity})
  end

  @doc """
  Returns a block **header** for a given block hash. With `verbose: false` (the
  default) the result is the 80-byte header as a 160-char hex string — small even
  for large blocks (unlike `get_block/2`, which returns the whole block).

  ## Returns
    - `{:ok, header_hex}` (verbose false) or `{:ok, header_map}` (verbose true)
    - `{:error, reason}` — on RPC or network failure
  """
  def get_block_header(hash_hex, verbose \\ false) do
    GenServer.call(__MODULE__, {:get_block_header, hash_hex, verbose})
  end

  @doc """
  Returns raw transaction data for a given txid.

  ## Parameters
    - `txid_hex` — 64-character hex string of the transaction ID
    - `verbose` — when true returns JSON object, when false returns hex string. Defaults to true.

  ## Returns
    - `{:ok, tx_data}` — map (verbose=true) or hex string (verbose=false)
    - `{:error, reason}` — on RPC or network failure
  """
  def get_raw_transaction(txid_hex, verbose \\ true) do
    GenServer.call(__MODULE__, {:get_raw_transaction, txid_hex, verbose})
  end

  @doc """
  Broadcasts a raw transaction hex to the BSV network.

  ## Parameters
    - `hex` — raw transaction in hex encoding

  ## Returns
    - `{:ok, txid}` — hex txid of the accepted transaction
    - `{:error, reason}` — on RPC or network failure (e.g. tx rejected)
  """
  def send_raw_transaction(hex) do
    GenServer.call(__MODULE__, {:send_raw_transaction, hex})
  end

  ## ── Server Callbacks ──

  @impl true
  def init(_opts) do
    config = Application.get_env(:athanor, :bsv_node, [])

    state = %{
      rpc_url: Keyword.get(config, :rpc_url, "http://localhost:18332"),
      rpc_user: Keyword.get(config, :rpc_user),
      rpc_password: Keyword.get(config, :rpc_password),
      request_id: 0
    }

    Logger.info("RpcClient started, url=#{state.rpc_url}")
    {:ok, state}
  end

  @impl true
  def handle_call(:get_block_count, _from, state) do
    {result, state} = rpc_call("getblockcount", [], state)
    {:reply, result, state}
  end

  def handle_call({:get_block_hash, height}, _from, state) do
    {result, state} = rpc_call("getblockhash", [height], state)
    {:reply, result, state}
  end

  def handle_call({:get_block_header, hash_hex, verbose}, _from, state) do
    {result, state} = rpc_call("getblockheader", [hash_hex, verbose], state)
    {:reply, result, state}
  end

  def handle_call({:get_block, hash_hex, verbosity}, _from, state) do
    {result, state} = rpc_call("getblock", [hash_hex, verbosity], state)
    {:reply, result, state}
  end

  def handle_call({:get_raw_transaction, txid_hex, verbose}, _from, state) do
    verbose_int = if verbose, do: 1, else: 0
    {result, state} = rpc_call("getrawtransaction", [txid_hex, verbose_int], state)
    {:reply, result, state}
  end

  def handle_call({:send_raw_transaction, hex}, _from, state) do
    {result, state} = rpc_call("sendrawtransaction", [hex], state)
    {:reply, result, state}
  end

  @impl true
  def handle_info(_msg, state) do
    {:noreply, state}
  end

  ## ── Private Helpers ──

  # Performs a JSON-RPC 2.0 call to the BSV node via HTTP POST with basic auth.
  #
  # ## Parameters
  #   - `method` — RPC method name (e.g. "getblockcount")
  #   - `params` — list of positional parameters for the RPC method
  #   - `state` — GenServer state containing connection config and request counter
  #
  # ## Returns
  #   `{result, updated_state}` where result is `{:ok, value}` or `{:error, reason}`
  defp rpc_call(method, params, state) do
    id = state.request_id + 1
    state = %{state | request_id: id}

    body = %{
      jsonrpc: "2.0",
      id: id,
      method: method,
      params: params
    }

    req_opts =
      [
        url: state.rpc_url,
        method: :post,
        json: body,
        receive_timeout: 30_000,
        finch: Athanor.Finch
      ]
      |> maybe_add_auth(state)

    case Req.request(req_opts) do
      {:ok, %Req.Response{status: 200, body: %{"result" => result, "error" => nil}}} ->
        {{:ok, result}, state}

      {:ok, %Req.Response{status: 200, body: %{"error" => %{"message" => msg, "code" => code}}}} ->
        Logger.warning("RPC error #{method}: code=#{code} msg=#{msg}")
        {{:error, {:rpc_error, code, msg}}, state}

      {:ok, %Req.Response{status: 200, body: %{"error" => error}}} when not is_nil(error) ->
        Logger.warning("RPC error #{method}: #{inspect(error)}")
        {{:error, {:rpc_error, error}}, state}

      {:ok, %Req.Response{status: status}} ->
        Logger.warning("RPC HTTP error #{method}: status=#{status}")
        {{:error, {:http_error, status}}, state}

      {:error, reason} ->
        Logger.warning("RPC request failed #{method}: #{inspect(reason)}")
        {{:error, {:request_failed, reason}}, state}
    end
  end

  # Adds basic auth header if credentials are configured.
  defp maybe_add_auth(opts, %{rpc_user: user, rpc_password: pass})
       when is_binary(user) and is_binary(pass) do
    encoded = Base.encode64("#{user}:#{pass}")
    Keyword.put(opts, :headers, [{"authorization", "Basic #{encoded}"}])
  end

  defp maybe_add_auth(opts, _state), do: opts
end
