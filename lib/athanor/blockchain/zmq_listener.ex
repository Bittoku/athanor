defmodule Athanor.Blockchain.ZmqListener do
  @moduledoc """
  ZMQ subscriber that listens for real-time notifications from the BSV node.

  Uses the chumak library (pure Erlang ZMQ implementation) to create SUB
  sockets that subscribe to BSV node ZMQ topics. On receipt of a message,
  forwards the payload to the appropriate downstream GenServer:

    - `rawtx`    → `Athanor.Indexer.TransactionFilter` via `{:process_raw_tx, payload}`
    - `hashblock` → `Athanor.Indexer.BlockProcessor` via `{:process_block_hash, payload}`

  Configuration is read from `Application.get_env(:athanor, :zmq)` which
  should provide `:raw_tx` and `:hash_block` endpoint strings (e.g. "tcp://127.0.0.1:28332").

  Connection is established asynchronously after init. On failure or disconnect,
  the listener schedules a reconnect attempt after a 5-second backoff.
  """

  use GenServer
  require Logger

  @reconnect_delay_ms 5_000

  ## ── Client API ──

  @doc """
  Starts the ZMQ listener GenServer. Reads ZMQ endpoints from app config.

  ## Parameters
    - `opts` — keyword options passed to GenServer.start_link

  ## Returns
    `{:ok, pid}` on success.
  """
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  ## ── Server Callbacks ──

  @impl true
  @doc """
  Initializes the listener by reading ZMQ config and creating SUB sockets
  via chumak. Sends a `:connect` message to self to begin async connection.

  ## State
    - `:rawtx_socket`       — chumak socket pid for rawtx subscription
    - `:hashblock_socket`   — chumak socket pid for hashblock subscription
    - `:raw_tx_endpoint`    — endpoint string like "tcp://127.0.0.1:28332"
    - `:hash_block_endpoint` — endpoint string like "tcp://127.0.0.1:28332"
    - `:connected`          — boolean indicating connection status
  """
  def init(_opts) do
    zmq_config = Application.get_env(:athanor, :zmq, [])

    raw_tx_endpoint = Keyword.get(zmq_config, :raw_tx)
    hash_block_endpoint = Keyword.get(zmq_config, :hash_block)

    # Create SUB sockets with unique identities
    rawtx_socket = create_sub_socket("athanor-rawtx")
    hashblock_socket = create_sub_socket("athanor-hashblock")

    state = %{
      rawtx_socket: rawtx_socket,
      hashblock_socket: hashblock_socket,
      raw_tx_endpoint: raw_tx_endpoint,
      hash_block_endpoint: hash_block_endpoint,
      connected: false
    }

    # Kick off async connection
    send(self(), :connect)

    Logger.info(
      "ZmqListener initialized, rawtx=#{raw_tx_endpoint}, hashblock=#{hash_block_endpoint}"
    )

    {:ok, state}
  end

  @impl true
  def handle_info(:connect, state) do
    case connect_and_subscribe(state) do
      {:ok, state} ->
        Logger.info("ZmqListener connected to BSV node ZMQ endpoints")
        {:noreply, %{state | connected: true}}

      {:error, reason} ->
        Logger.warning(
          "ZmqListener connection failed: #{inspect(reason)}, retrying in #{@reconnect_delay_ms}ms"
        )

        schedule_reconnect()
        {:noreply, %{state | connected: false}}
    end
  end

  # Handle ZMQ messages from chumak.
  # Chumak delivers multipart messages as a list: [topic, payload, sequence_number].
  def handle_info({:zmq, _socket, [<<"rawtx", _::binary>>, payload | _rest], _opts}, state) do
    Logger.debug("ZmqListener received rawtx (#{byte_size(payload)} bytes)")
    # Source-tag the observation (Phase 3 §A): ZMQ rawtx → `:zmq`.
    GenServer.cast(Athanor.Indexer.TransactionFilter, {:process_raw_tx, payload, :zmq})
    {:noreply, state}
  end

  def handle_info({:zmq, _socket, [<<"hashblock", _::binary>>, payload | _rest], _opts}, state) do
    Logger.debug("ZmqListener received hashblock (#{byte_size(payload)} bytes)")
    GenServer.cast(Athanor.Indexer.BlockProcessor, {:process_block_hash, payload})
    {:noreply, state}
  end

  # Catch-all for unrecognised ZMQ topics or other message formats
  def handle_info({:zmq, _socket, data, _opts}, state) do
    Logger.debug("ZmqListener received unknown ZMQ message: #{inspect(data, limit: 200)}")
    {:noreply, state}
  end

  def handle_info(:reconnect, state) do
    Logger.info("ZmqListener attempting reconnect...")
    send(self(), :connect)
    {:noreply, %{state | connected: false}}
  end

  def handle_info(msg, state) do
    Logger.debug("ZmqListener received unexpected message: #{inspect(msg, limit: 200)}")
    {:noreply, state}
  end

  ## ── Private Helpers ──

  # Creates a chumak SUB socket with the given identity string.
  #
  # ## Parameters
  #   - `identity` — unique string identifier for this socket
  #
  # ## Returns
  #   Socket pid on success, or nil if socket creation fails.
  defp create_sub_socket(identity) do
    case :chumak.socket(:sub, String.to_charlist(identity)) do
      {:ok, socket} ->
        socket

      {:error, reason} ->
        Logger.error("ZmqListener failed to create SUB socket '#{identity}': #{inspect(reason)}")
        nil
    end
  end

  # Connects both SUB sockets to their respective BSV node ZMQ endpoints
  # and subscribes to the appropriate topics.
  #
  # ## Parameters
  #   - `state` — current GenServer state with socket pids and endpoints
  #
  # ## Returns
  #   `{:ok, state}` on success, `{:error, reason}` on failure.
  defp connect_and_subscribe(state) do
    with {:rawtx_ep, endpoint} when is_binary(endpoint) <-
           {:rawtx_ep, state.raw_tx_endpoint},
         {:hashblock_ep, endpoint2} when is_binary(endpoint2) <-
           {:hashblock_ep, state.hash_block_endpoint},
         {:rawtx_sock, sock} when not is_nil(sock) <-
           {:rawtx_sock, state.rawtx_socket},
         {:hashblock_sock, sock2} when not is_nil(sock2) <-
           {:hashblock_sock, state.hashblock_socket},
         {rawtx_host, rawtx_port} <- parse_endpoint(state.raw_tx_endpoint),
         {hashblock_host, hashblock_port} <- parse_endpoint(state.hash_block_endpoint),
         {:ok, _} <- :chumak.connect(state.rawtx_socket, :tcp, rawtx_host, rawtx_port),
         :ok <- :chumak.subscribe(state.rawtx_socket, "rawtx"),
         {:ok, _} <-
           :chumak.connect(state.hashblock_socket, :tcp, hashblock_host, hashblock_port),
         :ok <- :chumak.subscribe(state.hashblock_socket, "hashblock") do
      {:ok, state}
    else
      {:rawtx_ep, nil} ->
        {:error, :raw_tx_endpoint_not_configured}

      {:hashblock_ep, nil} ->
        {:error, :hash_block_endpoint_not_configured}

      {:rawtx_sock, nil} ->
        {:error, :rawtx_socket_creation_failed}

      {:hashblock_sock, nil} ->
        {:error, :hashblock_socket_creation_failed}

      {:error, reason} ->
        {:error, reason}

      error ->
        {:error, {:unexpected, error}}
    end
  end

  # Parses a ZMQ endpoint string like "tcp://127.0.0.1:28332" into
  # a {host_charlist, port_integer} tuple suitable for :chumak.connect/4.
  #
  # ## Parameters
  #   - `endpoint` — endpoint string in "tcp://host:port" format
  #
  # ## Returns
  #   `{host_charlist, port_integer}` tuple.
  defp parse_endpoint(endpoint) do
    uri = URI.parse(endpoint)
    host = String.to_charlist(uri.host || "127.0.0.1")
    port = uri.port || 28332
    {host, port}
  end

  # Schedules a reconnect attempt after the configured delay.
  defp schedule_reconnect do
    Process.send_after(self(), :reconnect, @reconnect_delay_ms)
  end
end
