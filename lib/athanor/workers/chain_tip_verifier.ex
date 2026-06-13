defmodule Athanor.Workers.ChainTipVerifier do
  @moduledoc """
  Periodically verifies chain tip consistency by comparing local state
  against the BSV node. Detects reorgs and triggers catch-up if behind.
  """

  use GenServer
  require Logger

  alias Athanor.Repo
  alias Athanor.Schema.BlockProcessContext
  alias Athanor.Blockchain.RpcClient
  alias Athanor.Indexer.BlockProcessor
  import Ecto.Query

  @check_interval :timer.minutes(2)

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    schedule_check()
    {:ok, %{last_check: nil, consecutive_synced: 0}}
  end

  @impl true
  def handle_info(:verify_tip, state) do
    Logger.debug("ChainTipVerifier: verifying chain tip")
    state = verify_chain_tip(state)
    schedule_check()
    {:noreply, %{state | last_check: DateTime.utc_now()}}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  ## ── Private ──

  defp schedule_check do
    Process.send_after(self(), :verify_tip, @check_interval)
  end

  defp verify_chain_tip(state) do
    with {:ok, node_height} <- RpcClient.get_block_count(),
         {:ok, _node_hash} <- RpcClient.get_block_hash(node_height) do
      local_tip =
        BlockProcessContext
        |> order_by([b], desc: b.height)
        |> limit(1)
        |> Repo.one()

      local_height = if local_tip, do: local_tip.height, else: 0

      cond do
        local_height == node_height ->
          # In sync
          %{state | consecutive_synced: state.consecutive_synced + 1}

        local_height < node_height ->
          # Behind — feed missing block hashes to BlockProcessor
          Logger.info(
            "ChainTipVerifier: #{node_height - local_height} blocks behind, catching up"
          )

          catch_up(local_height + 1, node_height)
          %{state | consecutive_synced: 0}

        local_height > node_height ->
          # Ahead of node? Possible reorg
          Logger.warning(
            "ChainTipVerifier: local height #{local_height} > node #{node_height}, possible reorg"
          )

          %{state | consecutive_synced: 0}
      end
    else
      {:error, reason} ->
        Logger.warning("ChainTipVerifier: failed to verify tip: #{inspect(reason)}")
        state
    end
  end

  defp catch_up(from_height, to_height) when from_height > to_height, do: :ok

  defp catch_up(from_height, to_height) do
    # Process up to 10 blocks per cycle to avoid blocking
    max_height = min(from_height + 9, to_height)

    Enum.each(from_height..max_height, fn height ->
      case RpcClient.get_block_hash(height) do
        {:ok, hash_hex} ->
          case Base.decode16(hash_hex, case: :mixed) do
            {:ok, hash_binary} ->
              GenServer.cast(BlockProcessor, {:process_block_hash, hash_binary})

            :error ->
              Logger.warning("ChainTipVerifier: invalid block hash at height #{height}")
          end

        {:error, reason} ->
          Logger.warning("ChainTipVerifier: failed to get hash for #{height}: #{inspect(reason)}")
      end
    end)
  end
end
