defmodule Athanor.Workers.MissingTxSyncer do
  @moduledoc """
  Backfills missing transactions by querying an external history source
  (WhatsOnChain by default) for each watched address, then fetching any
  txids that are not already present in the local `meta_transactions` table.

  The scheduling shell delegates to `sync_once/1`, which accepts injectable
  history / raw-tx fetchers and a processor callback so it can be unit-tested
  without live network access.
  """

  use GenServer
  require Logger

  alias Athanor.Repo
  alias Athanor.Schema.{WatchingAddress, MetaTransaction}
  alias Athanor.Infra.WhatsOnChain
  alias Athanor.Indexer.TransactionFilter

  @sync_interval :timer.minutes(15)

  ## ── Client API ──

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @typedoc "Per-call summary returned by `sync_once/1`."
  @type summary :: %{
          addresses: non_neg_integer(),
          backfilled: non_neg_integer(),
          already_known: non_neg_integer(),
          errors: non_neg_integer()
        }

  @doc """
  Runs one sync pass over every watched address.

  ## Options
    - `:history_fetcher` — `(address -> {:ok, [txid_hex]} | {:error, term})`.
      Defaults to `WhatsOnChain.get_address_history/1`.
    - `:raw_tx_fetcher` — `(txid_hex -> {:ok, raw_hex} | {:error, term})`.
      Defaults to `WhatsOnChain.get_raw_tx/1`.
    - `:processor` — `(raw_binary -> any)` invoked for each backfilled tx.
      Defaults to a processor that source-tags the observation `:whatsonchain`
      (Phase 3 §A) via `TransactionFilter.process_raw_tx/2`.

  ## Returns
    A `summary/0` map counting watched addresses scanned, txs backfilled,
    txs already known, and errors encountered.
  """
  @spec sync_once(keyword()) :: summary()
  def sync_once(opts \\ []) do
    history_fetcher = Keyword.get(opts, :history_fetcher, &WhatsOnChain.get_address_history/1)
    raw_tx_fetcher = Keyword.get(opts, :raw_tx_fetcher, &WhatsOnChain.get_raw_tx/1)
    processor = Keyword.get(opts, :processor, &default_processor/1)

    addresses = Repo.all(WatchingAddress)

    base = %{addresses: length(addresses), backfilled: 0, already_known: 0, errors: 0}

    Enum.reduce(addresses, base, fn wa, acc ->
      case history_fetcher.(wa.address) do
        {:ok, txids} when is_list(txids) ->
          backfill_each(txids, raw_tx_fetcher, processor, acc)

        {:ok, _other} ->
          %{acc | errors: acc.errors + 1}

        {:error, reason} ->
          Logger.debug(
            "MissingTxSyncer: history fetch for #{wa.address} failed: #{inspect(reason)}"
          )

          %{acc | errors: acc.errors + 1}
      end
    end)
  end

  ## ── Server Callbacks ──

  @impl true
  def init(_opts) do
    schedule_sync()
    {:ok, %{last_sync: nil}}
  end

  @impl true
  def handle_info(:sync_missing, state) do
    Logger.debug("MissingTxSyncer: checking for missing transactions")
    sync_once()
    schedule_sync()
    {:noreply, %{state | last_sync: DateTime.utc_now()}}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  ## ── Private ──

  defp schedule_sync do
    Process.send_after(self(), :sync_missing, @sync_interval)
  end

  # The production processor: this backfill path fetches the raw tx from
  # WhatsOnChain, so the observation is source-tagged `:whatsonchain` (Phase 3 §A).
  defp default_processor(raw_binary),
    do: TransactionFilter.process_raw_tx(raw_binary, :whatsonchain)

  defp backfill_each(txids, raw_tx_fetcher, processor, acc) do
    Enum.reduce(txids, acc, fn txid_hex, acc ->
      with {:ok, txid_binary} <- Base.decode16(txid_hex, case: :mixed),
           nil <- Repo.get_by(MetaTransaction, txid: txid_binary) do
        backfill_one(txid_hex, raw_tx_fetcher, processor, acc)
      else
        :error ->
          %{acc | errors: acc.errors + 1}

        %MetaTransaction{} ->
          %{acc | already_known: acc.already_known + 1}
      end
    end)
  end

  defp backfill_one(txid_hex, raw_tx_fetcher, processor, acc) do
    case raw_tx_fetcher.(txid_hex) do
      {:ok, raw_hex} ->
        case Base.decode16(raw_hex, case: :mixed) do
          {:ok, raw_binary} ->
            Logger.info("MissingTxSyncer: backfilling tx #{txid_hex}")
            processor.(raw_binary)
            %{acc | backfilled: acc.backfilled + 1}

          :error ->
            %{acc | errors: acc.errors + 1}
        end

      {:error, _reason} ->
        %{acc | errors: acc.errors + 1}
    end
  end
end
