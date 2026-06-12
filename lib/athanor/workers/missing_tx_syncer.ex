defmodule Athanor.Workers.MissingTxSyncer do
  @moduledoc """
  Backfills missing transactions by querying JungleBus or WhatsOnChain.
  Handles gaps in the local index caused by downtime or missed ZMQ messages.
  """

  use GenServer
  require Logger

  alias Athanor.Repo
  alias Athanor.Schema.{WatchingAddress, MetaTransaction}
  alias Athanor.Infra.WhatsOnChain
  alias Athanor.Indexer.TransactionFilter

  @sync_interval :timer.minutes(15)

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    schedule_sync()
    {:ok, %{last_sync: nil}}
  end

  @impl true
  def handle_info(:sync_missing, state) do
    Logger.debug("MissingTxSyncer: checking for missing transactions")
    sync_watched_addresses()
    schedule_sync()
    {:noreply, %{state | last_sync: DateTime.utc_now()}}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  ## ── Private ──

  defp schedule_sync do
    Process.send_after(self(), :sync_missing, @sync_interval)
  end

  defp sync_watched_addresses do
    addresses = Repo.all(WatchingAddress)

    Enum.each(addresses, fn wa ->
      case WhatsOnChain.get_address_history(wa.address) do
        {:ok, txids} ->
          check_and_backfill(txids)

        {:error, reason} ->
          Logger.debug(
            "MissingTxSyncer: failed to fetch history for #{wa.address}: #{inspect(reason)}"
          )
      end
    end)
  end

  defp check_and_backfill(txids) when is_list(txids) do
    Enum.each(txids, fn txid_hex ->
      case Base.decode16(txid_hex, case: :mixed) do
        {:ok, txid_binary} ->
          # Check if we already have this tx
          unless Repo.get_by(MetaTransaction, txid: txid_binary) do
            Logger.info("MissingTxSyncer: backfilling tx #{txid_hex}")

            case WhatsOnChain.get_raw_tx(txid_hex) do
              {:ok, raw_hex} ->
                case Base.decode16(raw_hex, case: :mixed) do
                  {:ok, raw_binary} ->
                    # Source-tag the observation (Phase 3 §A): this backfill path
                    # fetches the raw tx from WhatsOnChain → `:whatsonchain`.
                    TransactionFilter.process_raw_tx(raw_binary, :whatsonchain)

                  :error ->
                    :ok
                end

              {:error, _} ->
                :ok
            end
          end

        :error ->
          :ok
      end
    end)
  end

  defp check_and_backfill(_), do: :ok
end
