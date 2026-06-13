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
  The single, synchronous, ordered index-mutation op (Phase 7 F7.2 T7.1). Rolls the
  index back to `:rollback_to` (skipped when `nil`), setting `last_height` to the
  fork height **before** connecting, then applies the **contiguous** new branch
  `:connect` (display-order block-hash binaries, ancestor→tip), **halting at the
  first connect failure**. Serialized in the `BlockProcessor` mailbox (a `call`, so
  it returns its result for the controller's state machine).

  ## Returns
    * `{:ok, last_height}` — rolled back (if any) and applied every connect block;
    * `{:partial, last_height, reason}` — rolled back and/or applied a prefix, then
      halted at `reason`; `last_height` is the last contiguous success;
    * `{:error, reason}` — nothing was mutated (no rollback and the first connect
      failed).
  """
  @spec apply_branch(GenServer.server(), %{
          required(:rollback_to) => non_neg_integer() | nil,
          required(:connect) => [binary()]
        }) ::
          {:ok, non_neg_integer()}
          | {:partial, non_neg_integer(), term()}
          | {:error, term()}
  def apply_branch(server \\ __MODULE__, %{rollback_to: rollback_to, connect: connect}) do
    GenServer.call(server, {:apply_branch, rollback_to, connect})
  end

  @doc """
  Records the **bootstrap anchor** — the one-time, context-only `block_process_contexts`
  row that seeds the index at its configured boundary (Phase 7 F7.2 T7.S). Unlike a
  normal block, the anchor's pre-bootstrap transactions are *not* indexed (the thin
  indexer starts from here), so this is a plain insert with no RPC fetch. Idempotent.
  Keeping it in `BlockProcessor` preserves the invariant that this module is the only
  writer of `block_process_contexts`. Returns `{:ok, height}`.

  ## Parameters
    - `height` — the bootstrap block height.
    - `hash` — the bootstrap block's display-order hash (lowercase hex), the row id.
  """
  @spec record_bootstrap_anchor(GenServer.server(), non_neg_integer(), String.t()) ::
          {:ok, non_neg_integer()}
  def record_bootstrap_anchor(server \\ __MODULE__, height, hash) do
    GenServer.call(server, {:record_bootstrap_anchor, height, hash})
  end

  ## ── Server Callbacks ──

  @impl true
  def init(_opts) do
    last_height = get_last_processed_height()
    {:ok, %{last_height: last_height, processing: false}}
  end

  @impl true
  def handle_call(:last_processed_height, _from, state) do
    {:reply, state.last_height, state}
  end

  def handle_call({:record_bootstrap_anchor, height, hash}, _from, state) do
    %BlockProcessContext{}
    |> BlockProcessContext.changeset(%{
      id: hash,
      height: height,
      processed_at: DateTime.utc_now()
    })
    |> Repo.insert(on_conflict: :nothing)

    {:reply, {:ok, height}, %{state | last_height: max(state.last_height, height)}}
  end

  def handle_call({:apply_branch, rollback_to, connect_hashes}, _from, state) do
    {mutated?, state} =
      if is_integer(rollback_to) do
        Logger.warning("BlockProcessor: rolling back to height #{rollback_to}")
        rollback_to(rollback_to)
        {true, %{state | last_height: rollback_to, processing: false}}
      else
        {false, state}
      end

    {result, state} = connect_branch(connect_hashes, state, mutated?: mutated?)
    {:reply, result, state}
  end

  # The public `{:process_block_hash, …}` mutation cast is **removed** (note-1035 B1):
  # a named-GenServer cast is externally reachable and would bypass the predecessor
  # guard + result contract. The only index-mutation path is `apply_branch/2`; any
  # stray cast falls through here and is a no-op (it cannot write contexts).
  @impl true
  def handle_cast(_msg, state), do: {:noreply, state}

  @impl true
  def handle_info(_msg, state) do
    {:noreply, state}
  end

  # Connects a contiguous canonical branch onto `state`, halting at the first failed
  # connect, and reports the result contract (see `apply_branch/2`). `opts`:
  # `:mutated?` (whether a rollback already changed the index this op — affects
  # `:partial` vs `:error`), `:process_fun` (`(hash, state -> {:ok, height} |
  # {:error, reason}`, injectable for tests; default the private block processor).
  # Returns `{result, state}`. Public (but `@doc false`) for unit tests only.
  @doc false
  @spec connect_branch([binary()], map(), keyword()) :: {term(), map()}
  def connect_branch(connect_hashes, state, opts \\ []) do
    mutated? = Keyword.get(opts, :mutated?, false)
    process_fun = Keyword.get(opts, :process_fun, &process_connect/2)
    do_connect(connect_hashes, state, mutated?, process_fun)
  end

  defp do_connect([], state, _mutated?, _fun), do: {{:ok, state.last_height}, state}

  defp do_connect([hash | rest], state, mutated?, fun) do
    case fun.(hash, state) do
      {:ok, height} ->
        do_connect(rest, %{state | last_height: height, processing: false}, true, fun)

      {:error, reason} ->
        result = if mutated?, do: {:partial, state.last_height, reason}, else: {:error, reason}
        {result, %{state | processing: false}}
    end
  end

  defp process_connect(hash, state) when is_binary(hash) do
    process_block(Base.encode16(hash, case: :lower), state)
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

  defp do_process_block(block_hash_hex) do
    with {:ok, block} <- RpcClient.get_block(block_hash_hex, 2),
         height = block["height"],
         prev_hash = block["previousblockhash"],
         # Reorg detection + no-gap bootstrap guard: a predecessor mismatch rolls
         # back and refuses the child; a missing predecessor is accepted only at the
         # configured bootstrap boundary (§5). Everything else is refused so a child
         # is never recorded over a gap.
         :ok <- predecessor_status(prev_hash, height, block_hash: block_hash_hex) do
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

  # Reorg detection + no-gap bootstrap predecessor guard (Phase 7 F7.2 T7.1) for a
  # block at `height` whose parent is `prev_hash`:
  #   * predecessor present and matching -> :ok;
  #   * predecessor present but a different hash -> roll back below it, then refuse
  #     the child ({:error, :missing_predecessor}) — the canonical branch must be
  #     reprocessed contiguously through apply_branch/2 first;
  #   * predecessor MISSING -> accepted (:ok) ONLY when this block is exactly the
  #     configured bootstrap block (height == bootstrap.height and, if the bootstrap
  #     is hash-pinned, block_hash == bootstrap.hash); otherwise refused.
  # `opts`: `:bootstrap` (%{height, hash} | nil, default the persisted boundary),
  # `:block_hash` (this block's hex id, for the hash-pin check). Public (but
  # `@doc false`) for unit tests only.
  @doc false
  @spec predecessor_status(binary() | nil, non_neg_integer(), keyword()) ::
          :ok | {:error, :missing_predecessor}
  def predecessor_status(prev_hash, height, opts \\ [])

  def predecessor_status(prev_hash, height, opts) when is_binary(prev_hash) do
    expected_height = height - 1

    case Repo.get_by(BlockProcessContext, height: expected_height) do
      %{id: stored_hash} when stored_hash == prev_hash ->
        :ok

      %{id: stored_hash} ->
        Logger.warning(
          "REORG detected at height #{expected_height}: expected #{prev_hash}, have #{stored_hash}"
        )

        rollback_to(expected_height - 1)
        {:error, :missing_predecessor}

      nil ->
        bootstrap_ok(height, opts)
    end
  end

  def predecessor_status(nil, height, opts), do: bootstrap_ok(height, opts)

  # A missing predecessor is acceptable only at the configured bootstrap boundary.
  defp bootstrap_ok(height, opts) do
    bootstrap = Keyword.get(opts, :bootstrap, current_bootstrap())
    block_hash = Keyword.get(opts, :block_hash)

    cond do
      is_nil(bootstrap) ->
        {:error, :missing_predecessor}

      bootstrap.height != height ->
        {:error, :missing_predecessor}

      not is_nil(bootstrap.hash) and bootstrap.hash != block_hash ->
        {:error, :missing_predecessor}

      true ->
        :ok
    end
  end

  # The persisted bootstrap boundary (`IndexerBootstrap` row). `nil` until captured
  # at first start, in which case every missing predecessor is refused.
  defp current_bootstrap, do: Athanor.Indexer.Bootstrap.fetch()

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
