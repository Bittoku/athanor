defmodule Athanor.Indexer.BlockProcessor do
  @moduledoc """
  Processes blocks sequentially, confirms UTXOs, and detects chain reorgs.

  When a new block hash arrives via ZMQ, fetches block data from the RPC node,
  processes each transaction through the filter/indexer pipeline, updates
  confirmation status, and records the block in block_process_contexts.
  """

  use GenServer
  require Logger

  alias Athanor.Repo
  alias Athanor.Schema.{BlockProcessContext, MetaTransaction, Utxo}
  alias Athanor.Blockchain.RpcClient
  alias Athanor.Indexer.{TransactionFilter, TransactionProcessor}
  import Ecto.Query

  @doc "Starts the BlockProcessor GenServer."
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Get the last processed block height."
  def last_processed_height do
    GenServer.call(__MODULE__, :last_processed_height)
  end

  @doc """
  Applies a P2P reorg as a **single ordered mailbox operation**: roll the index
  back to `fork_height` (skipped when `nil` — no recorded common ancestor), then
  process the new branch `connect_hashes` (display-order block-hash binaries,
  ancestor→tip) in order.

  Routing rollback + new-branch enqueue through this one cast serializes them
  behind any already-queued `process_block_hash` work, so a rollback can never
  interleave with in-flight block writes (Phase 6 §C; Hermes !18 note 932).

  ## Parameters
    - `server` — the BlockProcessor (defaults to the registered name).
    - `fork_height` — common-ancestor height to roll back to, or `nil` to skip.
    - `connect_hashes` — new-branch block hashes (display order) to process.

  ## Returns
    `:ok` (cast).
  """
  @spec apply_reorg(GenServer.server(), non_neg_integer() | nil, [binary()]) :: :ok
  def apply_reorg(server \\ __MODULE__, fork_height, connect_hashes) do
    GenServer.cast(server, {:apply_reorg, fork_height, connect_hashes})
  end

  ## ── Server Callbacks ──

  @impl true
  def init(_opts) do
    last_height = get_last_processed_height()
    {:ok, %{last_height: last_height, processing: false}}
  end

  @impl true
  def handle_cast({:process_block_hash, block_hash_binary}, state) do
    block_hash_hex = Base.encode16(block_hash_binary, case: :lower)
    Logger.info("BlockProcessor received block hash: #{block_hash_hex}")

    case process_block(block_hash_hex, state) do
      {:ok, height} ->
        {:noreply, %{state | last_height: height, processing: false}}

      {:error, reason} ->
        Logger.error("BlockProcessor failed: #{inspect(reason)}")
        {:noreply, %{state | processing: false}}
    end
  end

  def handle_cast({:apply_reorg, fork_height, connect_hashes}, state) do
    # After a successful rollback, `last_height` MUST drop to the fork height before
    # connecting the new branch — otherwise an empty/failed connect would leave the
    # GenServer advertising the orphaned tip height while the DB is rolled back
    # (Hermes !18 note 941 B3). Each connect block then advances it; a failing
    # connect leaves it at the last successfully processed height.
    state =
      if is_integer(fork_height) do
        Logger.warning("BlockProcessor: P2P reorg — rolling back to height #{fork_height}")
        rollback_to(fork_height)
        %{state | last_height: fork_height, processing: false}
      else
        state
      end

    {:noreply, connect_branch(connect_hashes, state)}
  end

  @doc """
  Connects a contiguous canonical branch (display-order block-hash binaries,
  ancestor→tip) onto `state`, **halting at the first failed connect** so a
  transient failure for block `N` can never let `N+1` be recorded over a gap
  (Hermes !18 note 945 B3). `last_height` ends at the last successfully processed
  contiguous height. `process_fun` is injectable for testing; it defaults to the
  real per-block processor.

  ## Returns
    The updated `state`.
  """
  @spec connect_branch(
          [binary()],
          map(),
          (binary(), map() -> {:ok, non_neg_integer()} | {:error, term()})
        ) ::
          map()
  def connect_branch(connect_hashes, state, process_fun \\ &process_connect/2) do
    Enum.reduce_while(connect_hashes, state, fn hash, acc ->
      case process_fun.(hash, acc) do
        {:ok, height} ->
          {:cont, %{acc | last_height: height, processing: false}}

        {:error, reason} ->
          Logger.error("BlockProcessor: reorg connect block failed (#{inspect(reason)}); halting")
          {:halt, %{acc | processing: false}}
      end
    end)
  end

  @impl true
  def handle_call(:last_processed_height, _from, state) do
    {:reply, state.last_height, state}
  end

  @impl true
  def handle_info(_msg, state) do
    {:noreply, state}
  end

  ## ── Private ──

  defp process_block(block_hash_hex, state) do
    # Check if already processed
    case Repo.get(BlockProcessContext, block_hash_hex) do
      %BlockProcessContext{} ->
        Logger.debug("Block #{block_hash_hex} already processed, skipping")
        {:ok, state.last_height}

      nil ->
        do_process_block(block_hash_hex)
    end
  end

  defp process_connect(hash, state) when is_binary(hash) do
    process_block(Base.encode16(hash, case: :lower), state)
  end

  defp do_process_block(block_hash_hex) do
    with {:ok, block} <- RpcClient.get_block(block_hash_hex, 2),
         height = block["height"],
         prev_hash = block["previousblockhash"],
         # Reorg detection + no-gap guard: a non-genesis block whose predecessor
         # context is missing (on a non-empty index) is refused, so a transient
         # failure for an earlier height can never record a child over a gap.
         :ok <- maybe_handle_reorg(prev_hash, height) do
      # Process each transaction in the block
      txids = block["tx"] || []

      Enum.each(txids, fn tx_data ->
        process_block_tx(tx_data, height, block_hash_hex)
      end)

      # Confirm any unconfirmed txs that are in this block
      confirm_block_txs(txids, height, block_hash_hex)

      # Record block as processed
      %BlockProcessContext{}
      |> BlockProcessContext.changeset(%{
        id: block_hash_hex,
        height: height,
        processed_at: DateTime.utc_now()
      })
      |> Repo.insert(on_conflict: :nothing)

      Logger.info("Processed block #{height} (#{block_hash_hex})")
      {:ok, height}
    else
      {:error, :missing_predecessor} = err ->
        Logger.warning(
          "BlockProcessor: refusing block #{block_hash_hex} — predecessor context missing (no-gap guard)"
        )

        err

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp process_block_tx(tx_data, _height, _block_hash_hex) when is_map(tx_data) do
    # Block verbosity=2 gives us full tx data as maps
    txid_hex = tx_data["txid"] || tx_data["hash"]

    case RpcClient.get_raw_transaction(txid_hex, false) do
      {:ok, raw_hex} ->
        case Base.decode16(raw_hex, case: :mixed) do
          {:ok, raw_binary} ->
            case BSV.Transaction.from_binary(raw_binary) do
              {:ok, tx, _rest} ->
                {matched_addrs, matched_tokens} = TransactionFilter.matches?(tx)

                if matched_addrs != [] or matched_tokens != [] do
                  # Source-tag the observation (Phase 3 §A): block indexing → `:block`.
                  TransactionProcessor.process_tx(tx, matched_addrs, matched_tokens, :block)
                end

              _ ->
                :ok
            end

          :error ->
            :ok
        end

      {:error, _} ->
        :ok
    end
  end

  defp process_block_tx(txid_hex, height, block_hash_hex) when is_binary(txid_hex) do
    # Block verbosity=1 gives us just txid strings
    process_block_tx(%{"txid" => txid_hex}, height, block_hash_hex)
  end

  defp confirm_block_txs(txids, height, block_hash_hex) do
    tx_hex_ids =
      Enum.map(txids, fn
        %{"txid" => txid} -> txid
        txid when is_binary(txid) -> txid
      end)

    # Update MetaTransactions
    Enum.each(tx_hex_ids, fn txid_hex ->
      case Base.decode16(txid_hex, case: :mixed) do
        {:ok, txid_binary} ->
          MetaTransaction
          |> where([m], m.txid == ^txid_binary)
          |> Repo.update_all(
            set: [
              is_confirmed: true,
              block_height: height,
              block_hash: Base.decode16!(block_hash_hex, case: :mixed)
            ]
          )

          # Confirm UTXOs
          Utxo
          |> where([u], u.txid == ^txid_binary)
          |> Repo.update_all(set: [block_height: height])

        :error ->
          :ok
      end
    end)
  end

  # Reorg detection + no-gap predecessor guard for a block at `height` whose parent
  # is `prev_hash` (Hermes !18 note 945 B3):
  #   * predecessor context present and matching -> :ok (chain consistent);
  #   * predecessor context present but a different hash -> reorg: roll back below
  #     it, then :ok;
  #   * predecessor context missing:
  #       - on an EMPTY index -> :ok (the genuine first block has no predecessor);
  #       - on a NON-EMPTY index -> {:error, :missing_predecessor}: recording this
  #         block would leave a gap (its parent was never processed) -> refused.
  # Public (but `@doc false`) only so the no-gap guard is unit-testable.
  @doc false
  @spec maybe_handle_reorg(binary() | nil, non_neg_integer()) ::
          :ok | {:error, :missing_predecessor}
  def maybe_handle_reorg(prev_hash, height) when is_binary(prev_hash) do
    expected_height = height - 1

    case Repo.get_by(BlockProcessContext, height: expected_height) do
      nil ->
        # No predecessor context. Allow only when the index is empty (a genuine
        # cold start at this height); otherwise refuse — recording over the gap
        # would let a child be stored without its parent.
        if index_empty?(), do: :ok, else: {:error, :missing_predecessor}

      %{id: stored_hash} when stored_hash == prev_hash ->
        # Chain is consistent
        :ok

      %{id: stored_hash} ->
        # Reorg detected!
        Logger.warning(
          "REORG detected at height #{expected_height}: expected #{prev_hash}, have #{stored_hash}"
        )

        rollback_to(expected_height - 1)
        :ok
    end
  end

  def maybe_handle_reorg(nil, _height), do: :ok

  defp index_empty?, do: not Repo.exists?(BlockProcessContext)

  @doc """
  Rolls the index back to `height` after a chain reorg.

  Every transaction in an orphaned block (`block_height > height`) is
  demoted to the unconfirmed state rather than deleted — if the new
  chain re-mines it, normal processing re-confirms it. Critically, any
  UTXO that an orphaned transaction *spent* is freed (`is_spent` reset
  to false): otherwise the UTXO set would retain a phantom spend for a
  transaction that may never reappear on the new chain.

  Transactions at or below `height` are left untouched.
  """
  def rollback_to(height) do
    Logger.warning("Rolling back to height #{height}")

    # Capture the orphaned txids BEFORE the un-confirm below clears the
    # block_height column this query filters on.
    orphaned_txids =
      MetaTransaction
      |> where([m], m.block_height > ^height)
      |> select([m], m.txid)
      |> Repo.all()

    # Free parent UTXOs that orphaned txs had spent — they return to the
    # unspent set since the spending tx is no longer on the active chain.
    if orphaned_txids != [] do
      Utxo
      |> where([u], u.spent_txid in ^orphaned_txids)
      |> Repo.update_all(set: [is_spent: false, spent_txid: nil])
    end

    # Delete block process contexts above this height
    BlockProcessContext
    |> where([b], b.height > ^height)
    |> Repo.delete_all()

    # Unconfirm transactions above this height
    MetaTransaction
    |> where([m], m.block_height > ^height)
    |> Repo.update_all(set: [is_confirmed: false, block_height: nil, block_hash: nil])

    # Unconfirm UTXOs above this height
    Utxo
    |> where([u], u.block_height > ^height)
    |> Repo.update_all(set: [block_height: nil])
  end

  defp get_last_processed_height do
    BlockProcessContext
    |> order_by([b], desc: b.height)
    |> limit(1)
    |> select([b], b.height)
    |> Repo.one() || 0
  end
end
