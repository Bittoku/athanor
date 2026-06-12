defmodule Athanor.Blockchain.JungleBusClient do
  @moduledoc """
  Client for JungleBus — GorillaPool's BSV transaction indexer.

  JungleBus provides REST endpoints for:
  - Block headers: `/v1/block_header/get/{height}`
  - Transaction lookup: `/v1/transaction/get/{txid}`
  - Address transactions (paginated): fetched via block scanning

  When a subscription_id is configured, streams mined + mempool transactions
  via Server-Sent Events (SSE).

  Used as an alternative/supplement to direct ZMQ from a BSV node.
  """

  use GenServer
  require Logger

  alias Athanor.Indexer.TransactionFilter

  @default_url "https://junglebus.gorillapool.io"
  @poll_interval :timer.seconds(30)

  ## ── Client API ──

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Fetch a block header by height."
  def get_block_header(height) do
    GenServer.call(__MODULE__, {:get_block_header, height}, 15_000)
  end

  @doc "Fetch a transaction by txid."
  def get_transaction(txid_hex) do
    GenServer.call(__MODULE__, {:get_transaction, txid_hex}, 15_000)
  end

  @doc "Fetch raw transaction hex by txid."
  def get_raw_transaction(txid_hex) do
    case get_transaction(txid_hex) do
      {:ok, %{"transaction" => base64_tx}} when is_binary(base64_tx) ->
        case Base.decode64(base64_tx) do
          {:ok, raw_binary} -> {:ok, Base.encode16(raw_binary, case: :lower)}
          :error -> {:error, :invalid_base64}
        end

      {:ok, _} ->
        {:error, :no_transaction_data}

      {:error, _} = err ->
        err
    end
  end

  ## ── Server Callbacks ──

  @impl true
  def init(_opts) do
    config = Application.get_env(:athanor, :jungle_bus, [])

    state = %{
      enabled: Keyword.get(config, :enabled, false),
      url: Keyword.get(config, :url, @default_url),
      subscription_id: Keyword.get(config, :subscription_id),
      last_block_height: 0,
      polling: false
    }

    if state.enabled do
      Logger.info("JungleBusClient: enabled, URL=#{state.url}")

      if state.subscription_id do
        send(self(), :start_sse)
      else
        send(self(), :start_polling)
      end
    else
      Logger.info("JungleBusClient: disabled")
    end

    {:ok, state}
  end

  @impl true
  def handle_call({:get_block_header, height}, _from, state) do
    result = do_get("#{state.url}/v1/block_header/get/#{height}")
    {:reply, result, state}
  end

  def handle_call({:get_transaction, txid_hex}, _from, state) do
    result = do_get("#{state.url}/v1/transaction/get/#{txid_hex}")
    {:reply, result, state}
  end

  @impl true
  def handle_info(:start_polling, state) do
    Logger.info("JungleBusClient: starting block polling mode")
    send(self(), :poll_blocks)
    {:noreply, %{state | polling: true}}
  end

  def handle_info(:poll_blocks, %{enabled: false} = state), do: {:noreply, state}

  def handle_info(:poll_blocks, state) do
    state = poll_for_new_blocks(state)
    Process.send_after(self(), :poll_blocks, @poll_interval)
    {:noreply, state}
  end

  def handle_info(:start_sse, state) do
    Logger.info("JungleBusClient: starting SSE subscription #{state.subscription_id}")
    # Start SSE streaming in a linked task
    parent = self()

    Task.start_link(fn ->
      stream_sse(state.url, state.subscription_id, state.last_block_height, parent)
    end)

    {:noreply, state}
  end

  def handle_info({:sse_transaction, tx_data}, state) do
    process_junglebus_tx(tx_data)
    {:noreply, state}
  end

  def handle_info({:sse_status, %{"block" => height}}, state) do
    Logger.debug("JungleBusClient: SSE synced to block #{height}")
    {:noreply, %{state | last_block_height: height}}
  end

  def handle_info({:sse_error, reason}, state) do
    Logger.warning("JungleBusClient: SSE error: #{inspect(reason)}, restarting in 10s")
    Process.send_after(self(), :start_sse, 10_000)
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  ## ── REST API ──

  defp do_get(url) do
    case Req.get(url, finch: Athanor.Finch, receive_timeout: 10_000) do
      {:ok, %Req.Response{status: 200, body: body}} when is_map(body) ->
        {:ok, body}

      {:ok, %Req.Response{status: 200, body: body}} when is_binary(body) ->
        case Jason.decode(body) do
          {:ok, decoded} -> {:ok, decoded}
          {:error, _} -> {:ok, body}
        end

      {:ok, %Req.Response{status: status}} ->
        {:error, {:http_error, status}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  ## ── Block Polling ──

  defp poll_for_new_blocks(state) do
    # Get the current chain tip from JungleBus
    case do_get("#{state.url}/v1/block_header/get/latest") do
      {:ok, %{"height" => tip_height}} when tip_height > state.last_block_height ->
        from = max(state.last_block_height + 1, tip_height - 5)
        to = tip_height

        Logger.info("JungleBusClient: polling blocks #{from}..#{to}")

        Enum.reduce(from..to, state, fn height, acc ->
          case do_get("#{state.url}/v1/block_header/get/#{height}") do
            {:ok, %{"hash" => hash}} ->
              # Feed to BlockProcessor
              case Base.decode16(hash, case: :mixed) do
                {:ok, hash_binary} ->
                  GenServer.cast(
                    Athanor.Indexer.BlockProcessor,
                    {:process_block_hash, hash_binary}
                  )

                :error ->
                  :ok
              end

              %{acc | last_block_height: height}

            {:error, reason} ->
              Logger.warning("JungleBusClient: failed to get block #{height}: #{inspect(reason)}")
              acc
          end
        end)

      {:ok, _} ->
        # No new blocks
        state

      {:error, reason} ->
        Logger.debug("JungleBusClient: poll failed: #{inspect(reason)}")
        state
    end
  end

  ## ── SSE Streaming ──

  defp stream_sse(base_url, subscription_id, from_block, parent) do
    url = "#{base_url}/v1/subscription/stream/#{subscription_id}?fromBlock=#{from_block}"

    Logger.info("JungleBusClient: connecting SSE to #{url}")

    case Req.get(url,
           finch: Athanor.Finch,
           receive_timeout: :infinity,
           into: :self
         ) do
      {:ok, resp} ->
        sse_receive_loop(resp, parent, "")

      {:error, reason} ->
        send(parent, {:sse_error, reason})
    end
  end

  defp sse_receive_loop(resp, parent, buffer) do
    receive do
      {ref, {:data, chunk}} when ref == resp.body ->
        # SSE format: "data: {json}\n\n"
        buffer = buffer <> chunk

        {events, remaining} = parse_sse_events(buffer)

        Enum.each(events, fn event ->
          case Jason.decode(event) do
            {:ok, %{"type" => "transaction"} = data} ->
              send(parent, {:sse_transaction, data})

            {:ok, %{"type" => "mempool"} = data} ->
              send(parent, {:sse_transaction, data})

            {:ok, %{"type" => "status"} = data} ->
              send(parent, {:sse_status, data})

            {:ok, _data} ->
              :ok

            {:error, _} ->
              :ok
          end
        end)

        sse_receive_loop(resp, parent, remaining)

      {ref, :done} when ref == resp.body ->
        Logger.info("JungleBusClient: SSE stream ended")
        send(parent, {:sse_error, :stream_ended})

      _other ->
        sse_receive_loop(resp, parent, buffer)
    after
      60_000 ->
        Logger.warning("JungleBusClient: SSE timeout, reconnecting")
        send(parent, {:sse_error, :timeout})
    end
  end

  defp parse_sse_events(buffer) do
    # Split on double newlines (SSE event boundary)
    parts = String.split(buffer, "\n\n")

    case parts do
      [single] ->
        # No complete event yet
        {[], single}

      parts ->
        {complete, [remaining]} = Enum.split(parts, -1)

        events =
          complete
          |> Enum.map(fn part ->
            part
            |> String.split("\n")
            |> Enum.filter(&String.starts_with?(&1, "data: "))
            |> Enum.map(&String.trim_leading(&1, "data: "))
            |> Enum.join("")
          end)
          |> Enum.reject(&(&1 == ""))

        {events, remaining}
    end
  end

  ## ── Transaction Processing ──

  defp process_junglebus_tx(%{"transaction" => base64_tx} = _data) when is_binary(base64_tx) do
    case Base.decode64(base64_tx) do
      {:ok, raw_binary} ->
        # Source-tag the observation (Phase 3 §A): JungleBus stream → `:junglebus`.
        TransactionFilter.process_raw_tx(raw_binary, :junglebus)

      :error ->
        Logger.warning("JungleBusClient: invalid base64 transaction data")
    end
  end

  defp process_junglebus_tx(%{"id" => txid}) do
    # Lite mode — just got a txid, fetch full tx
    Logger.debug("JungleBusClient: lite mode tx #{txid}, fetching full data")

    case do_get("#{@default_url}/v1/transaction/get/#{txid}") do
      {:ok, tx_data} -> process_junglebus_tx(tx_data)
      {:error, _} -> :ok
    end
  end

  defp process_junglebus_tx(_), do: :ok
end
