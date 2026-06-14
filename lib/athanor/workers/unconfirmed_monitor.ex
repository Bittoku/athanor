defmodule Athanor.Workers.UnconfirmedMonitor do
  @moduledoc """
  Periodically rechecks stale unconfirmed transactions to determine
  if they've been confirmed, dropped from mempool, or replaced.

  The scheduling shell (`init`, `handle_info(:check_unconfirmed, ...)`)
  is a thin wrapper. The sweep logic lives in `sweep/1`, which accepts
  an injectable fetcher so it can be unit-tested without a live BSV node.
  """

  use GenServer
  require Logger

  alias Athanor.Repo
  alias Athanor.Schema.MetaTransaction
  alias Athanor.Blockchain.RpcClient
  import Ecto.Query

  @check_interval :timer.minutes(5)
  @default_stale_threshold_ms :timer.hours(1)

  ## ── Client API ──

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @typedoc "Fetcher signature: `fn txid_hex -> {:ok, verbose_tx_map} | {:error, term} end`."
  @type fetcher :: (String.t() -> {:ok, map()} | {:error, term()})

  @doc """
  Sweeps the meta_transactions table for stale unconfirmed rows and updates
  each according to its current state on the BSV node.

  ## Options
    - `:fetcher` — `(txid_hex -> {:ok, tx_map} | {:error, reason})`.
      Defaults to `RpcClient.get_raw_transaction(txid_hex, true)`.
    - `:stale_threshold_ms` — only rows older than this are considered.
      Defaults to 1 hour.
    - `:pubsub` — Phoenix.PubSub server name for `:tx_deleted` broadcasts.
      Defaults to `Athanor.PubSub`.

  ## Returns
    A summary map: `%{confirmed: N, dropped: N, still_unconfirmed: N, errors: N}`
  """
  @spec sweep(keyword()) :: %{
          confirmed: non_neg_integer(),
          dropped: non_neg_integer(),
          still_unconfirmed: non_neg_integer(),
          errors: non_neg_integer()
        }
  def sweep(opts \\ []) do
    fetcher = Keyword.get(opts, :fetcher, &default_fetcher/1)
    stale_threshold_ms = Keyword.get(opts, :stale_threshold_ms, @default_stale_threshold_ms)
    pubsub = Keyword.get(opts, :pubsub, Athanor.PubSub)

    stale_cutoff = System.os_time(:second) - div(stale_threshold_ms, 1000)

    stale_txs =
      MetaTransaction
      |> where([m], m.is_confirmed == false and m.timestamp < ^stale_cutoff)
      |> limit(100)
      |> Repo.all()

    Enum.reduce(stale_txs, %{confirmed: 0, dropped: 0, still_unconfirmed: 0, errors: 0}, fn meta,
                                                                                            acc ->
      process_one(meta, fetcher, pubsub, acc)
    end)
  end

  ## ── Server Callbacks ──

  @impl true
  def init(_opts) do
    schedule_check()
    {:ok, %{last_check: nil}}
  end

  @impl true
  def handle_info(:check_unconfirmed, state) do
    Logger.debug("UnconfirmedMonitor: checking stale unconfirmed transactions")
    sweep()
    schedule_check()
    {:noreply, %{state | last_check: DateTime.utc_now()}}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  ## ── Private ──

  defp schedule_check do
    Process.send_after(self(), :check_unconfirmed, @check_interval)
  end

  defp default_fetcher(txid_hex), do: RpcClient.get_raw_transaction(txid_hex, true)

  defp process_one(meta, fetcher, pubsub, acc) do
    txid_hex = Base.encode16(meta.txid, case: :lower)

    case fetcher.(txid_hex) do
      {:ok, %{"blockhash" => block_hash, "confirmations" => conf}} when conf > 0 ->
        Logger.info("UnconfirmedMonitor: tx #{txid_hex} now confirmed")

        meta
        |> MetaTransaction.changeset(%{
          is_confirmed: true,
          block_hash: Base.decode16!(block_hash, case: :mixed)
        })
        |> Repo.update()

        %{acc | confirmed: acc.confirmed + 1}

      {:ok, _} ->
        %{acc | still_unconfirmed: acc.still_unconfirmed + 1}

      {:error, {:rpc_error, -5, _msg}} ->
        # bitcoind RPC error -5 = "No such mempool or blockchain transaction"
        # → tx is gone from the node's view (dropped from mempool, replaced, etc.)
        Logger.info(
          "UnconfirmedMonitor: tx #{txid_hex} dropped from mempool, publishing tx_deleted"
        )

        Phoenix.PubSub.broadcast(
          pubsub,
          "tx:#{txid_hex}",
          {:tx_deleted, %{txid: txid_hex}}
        )

        %{acc | dropped: acc.dropped + 1}

      {:error, reason} ->
        Logger.warning("UnconfirmedMonitor: failed to check tx #{txid_hex}: #{inspect(reason)}")
        %{acc | errors: acc.errors + 1}
    end
  end
end
