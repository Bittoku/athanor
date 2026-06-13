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
  alias Athanor.Indexer.{Bootstrap, TransactionFilter, TransactionProcessor}
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
    # `:infinity` (note-1061 B1): the ordered mutation may fetch blocks over RPC and do
    # per-block DB work; a bounded batch can legitimately exceed the default 5s call
    # timeout. A timeout would let the controller fold a false failure while this server
    # keeps mutating — so the caller waits for the authoritative synchronous result.
    GenServer.call(server, {:apply_branch, rollback_to, connect}, :infinity)
  end

  @doc """
  Captures the **bootstrap boundary + anchor atomically** (Phase 7 F7.2 T7.S,
  tightened per Hermes !20 note 1053). In a single DB transaction through this
  module — the only writer of `block_process_contexts` — it:

    1. ensures the capture-once `indexer_bootstrap` singleton (`Bootstrap.ensure/2`), and
    2. **on a fresh (empty) index**, inserts the context-only anchor row that seeds the
       index at the boundary (its pre-bootstrap transactions are *not* indexed — the
       thin indexer starts here).

  Because both writes share one transaction, a failed anchor insert rolls the
  singleton back too: a fresh install can never be left with a persisted-but-unanchored
  boundary that the no-gap predecessor guard would then reject forever. The op is
  **idempotent and self-healing** — re-running it ensures the singleton and, if the
  index is still empty (e.g. a prior crash stranded the singleton without an anchor),
  inserts the missing anchor; on a non-empty index it is a no-op. The result is
  **checked**: `{:error, reason}` on any failure so the caller does not treat the index
  as bootstrapped.

  ## Parameters
    - `height` — the bootstrap block height.
    - `hash` — the bootstrap block's display-order hash (lowercase hex), the row id.

  ## Returns
    `{:ok, height}` once the boundary is captured and the index is anchored (or was
    already non-empty); `{:error, reason}` if the transaction failed.
  """
  @spec capture_bootstrap(GenServer.server(), non_neg_integer(), String.t()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def capture_bootstrap(server \\ __MODULE__, height, hash) do
    # `:infinity` (note-1061 B1): the capture+anchor transaction runs through this
    # server and must not be abandoned by a default 5s call timeout while it commits.
    GenServer.call(server, {:capture_bootstrap, height, hash}, :infinity)
  end

  ## ── Server Callbacks ──

  @impl true
  def init(opts) do
    last_height = get_last_processed_height()
    # `:process_fun` ((hash, state) -> {:ok, height} | {:error, reason}) is the
    # per-block connect step; injectable so unit tests can drive a slow/refusing
    # processor without RPC. Defaults to the real block processor.
    process_fun = Keyword.get(opts, :process_fun, &process_connect/2)
    # `:rollback_fun` (height -> :ok | {:error, reason}) is the reorg rollback step;
    # injectable so a regression can drive a failing rollback. Defaults to the real
    # transactional `rollback_to/1`.
    rollback_fun = Keyword.get(opts, :rollback_fun, &rollback_to/1)

    {:ok,
     %{
       last_height: last_height,
       processing: false,
       process_fun: process_fun,
       rollback_fun: rollback_fun
     }}
  end

  @impl true
  def handle_call(:last_processed_height, _from, state) do
    {:reply, state.last_height, state}
  end

  def handle_call({:capture_bootstrap, height, hash}, _from, state) do
    # Singleton + anchor in ONE transaction: a failed anchor insert rolls the
    # boundary back too, so no persisted-but-unanchored singleton can survive
    # (note-1053). The anchor is inserted only on a fresh (empty) index — making the
    # op idempotent and self-healing for a previously-stranded singleton.
    result =
      Repo.transaction(fn ->
        empty? = Repo.aggregate(BlockProcessContext, :count) == 0
        Bootstrap.ensure(height, hash)

        if empty? do
          case insert_bootstrap_anchor(height, hash) do
            {:ok, _} -> :anchored
            {:error, reason} -> Repo.rollback({:anchor_insert_failed, reason})
          end
        else
          :already_anchored
        end
      end)

    case result do
      {:ok, :anchored} ->
        {:reply, {:ok, height}, %{state | last_height: max(state.last_height, height)}}

      {:ok, :already_anchored} ->
        {:reply, {:ok, height}, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:apply_branch, rollback_to, connect_hashes}, _from, state) do
    case apply_rollback(state, rollback_to) do
      {:error, reason} ->
        # The rollback failed before any connect work → fail closed: do NOT lower
        # `last_height` or connect; report the failure through the result contract
        # (note-1069 B2) so the controller defers rather than treating it as applied.
        {:reply, {:error, {:rollback_failed, reason}}, state}

      {:ok, mutated?, state} ->
        {result, state} =
          connect_branch(connect_hashes, state,
            mutated?: mutated?,
            process_fun: state.process_fun
          )

        {:reply, result, state}
    end
  end

  # Run the reconcile-supplied rollback (if any) through the fail-closed contract:
  # `last_height` drops to the fork height ONLY after a successful, transactional
  # rollback; a failure propagates so the caller does not claim a rollback that did
  # not durably happen (note-1069 B2).
  defp apply_rollback(state, rollback_to) when is_integer(rollback_to) do
    Logger.warning("BlockProcessor: rolling back to height #{rollback_to}")

    case state.rollback_fun.(rollback_to) do
      :ok -> {:ok, true, %{state | last_height: rollback_to, processing: false}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp apply_rollback(state, _no_rollback), do: {:ok, false, state}

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

  # Inserts the context-only bootstrap anchor row. `on_conflict: :nothing` keeps it
  # idempotent; the changeset/insert result is returned so the enclosing transaction
  # can roll back (and the caller can fail closed) on any error.
  defp insert_bootstrap_anchor(height, hash) do
    %BlockProcessContext{}
    |> BlockProcessContext.changeset(%{
      id: hash,
      height: height,
      processed_at: DateTime.utc_now()
    })
    |> Repo.insert(on_conflict: :nothing)
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
      # Process the block's txs, confirm them, and record the durable context — all in
      # ONE transaction (note-1057), returning {:ok, height} only once the context row
      # is durably present; a failed context insert returns {:error, reason}.
      record_block(block_hash_hex, height, block["tx"] || [])
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

  # Records a connected block ATOMICALLY (Phase 7 F7.2 T7.1, tightened per Hermes !20
  # note-1057): in ONE `Repo.transaction`, processes the block's transactions, confirms
  # them, and inserts the durable `block_process_contexts` row. The context insert
  # result is CHECKED — a failure (e.g. the unique-`height` constraint, or a changeset
  # error) rolls the per-block side effects back and returns
  # `{:error, {:context_insert_failed, _}}`, so `connect_branch/3` halts at this block
  # and `TipController` defers rather than treating the height as durably applied.
  # Returns `{:ok, height}` only once the context row is committed.
  #
  # `@doc false` — an internal step of `apply_branch/2` (whose `predecessor_status/3`
  # gate runs first); exposed for direct unit testing, like `connect_branch/3` and
  # `predecessor_status/3`. It is a plain function (no externally-reachable message), so
  # it does not reintroduce a mutation path that bypasses the ordered op (note-1035).
  @doc false
  @spec record_block(String.t(), non_neg_integer(), [map() | binary()], keyword()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def record_block(block_hash_hex, height, txids, opts \\ []) do
    # `:index_fun` ((tx_data, height, block_hash_hex) -> :ok | {:error, reason}) indexes
    # one block tx; default the real fetch+filter+index path. Injectable so a regression
    # can drive a matched-tx index without RPC. Crucially it runs INLINE — in this
    # process, inside the transaction below — so its MetaTransaction/UTXO/address-history
    # writes share the connection with the context insert (note-1065).
    index_fun = Keyword.get(opts, :index_fun, &process_block_tx/3)

    result =
      Repo.transaction(fn ->
        case index_block_txs(txids, height, block_hash_hex, index_fun) do
          {:error, reason} ->
            Repo.rollback({:tx_index_failed, reason})

          :ok ->
            confirm_block_txs(txids, height, block_hash_hex)

            case insert_block_context(block_hash_hex, height) do
              {:ok, _row} -> :recorded
              {:error, changeset} -> Repo.rollback({:context_insert_failed, changeset})
            end
        end
      end)

    case result do
      {:ok, :recorded} ->
        Logger.info("Processed block #{height} (#{block_hash_hex})")
        {:ok, height}

      {:error, reason} ->
        Logger.warning(
          "BlockProcessor: block #{block_hash_hex} not recorded (#{inspect(reason)}) — fail closed"
        )

        {:error, reason}
    end
  end

  # Index each block tx through `index_fun`, halting (and failing closed) at the first
  # indexing error so the caller's transaction rolls the whole per-block mutation back.
  defp index_block_txs([], _height, _block_hash_hex, _fun), do: :ok

  defp index_block_txs([tx_data | rest], height, block_hash_hex, fun) do
    case fun.(tx_data, height, block_hash_hex) do
      :ok -> index_block_txs(rest, height, block_hash_hex, fun)
      {:error, reason} -> {:error, reason}
    end
  end

  # Inserts the durable per-block context row, returning the `Repo.insert` result so
  # the enclosing transaction can roll back (and the caller fail closed) on any error.
  # No `on_conflict: :nothing`: a same-`height` (or duplicate-id) conflict is a real
  # error here — the block was not durably recorded and must not be reported applied.
  defp insert_block_context(block_hash_hex, height) do
    %BlockProcessContext{}
    |> BlockProcessContext.changeset(%{
      id: block_hash_hex,
      height: height,
      processed_at: DateTime.utc_now()
    })
    |> Repo.insert()
  end

  defp process_block_tx(tx_data, _height, _block_hash_hex) when is_map(tx_data) do
    # Block verbosity=2 gives us full tx data as maps
    txid_hex = tx_data["txid"] || tx_data["hash"]

    with {:ok, raw_hex} <- RpcClient.get_raw_transaction(txid_hex, false),
         {:ok, raw_binary} <- Base.decode16(raw_hex, case: :mixed),
         {:ok, tx, _rest} <- BSV.Transaction.from_binary(raw_binary) do
      {matched_addrs, matched_tokens} = TransactionFilter.matches?(tx)

      if matched_addrs != [] or matched_tokens != [] do
        # Source-tag the observation (Phase 3 §A): block indexing → `:block`. INLINE
        # via `index_tx/4` (note-1065): runs in THIS process so its writes share the
        # block-recording transaction; a failure propagates so the block fails closed.
        case TransactionProcessor.index_tx(tx, matched_addrs, matched_tokens, :block) do
          {:ok, _txid} -> :ok
          {:error, reason} -> {:error, reason}
        end
      else
        :ok
      end
    else
      # A tx we cannot fetch/decode is skipped (unchanged behavior) — that is an
      # un-indexable observation, not a block-recording failure.
      _ -> :ok
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

  # No-gap bootstrap predecessor guard (Phase 7 F7.2 T7.1) for a block at `height`
  # whose parent is `prev_hash`. **Read-only** — it never mutates the index (note-1061
  # B2), so a refusal honestly means "nothing mutated":
  #   * predecessor present and matching -> :ok;
  #   * predecessor present but a different hash -> refuse the child
  #     ({:error, :missing_predecessor}) WITHOUT rolling back; the divergence is
  #     corrected by the reconcile-supplied `rollback_to` in a later apply_branch/2
  #     (the single rollback authority), not here;
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
        # A divergent predecessor is detected and REFUSED, but NOT rolled back here
        # (note-1061 B2): rolling back while returning the "nothing mutated" error shape
        # corrupts the result contract and leaves `last_height` stale. The reconcile
        # cycle observes the divergence (by hash) and supplies the authoritative
        # `rollback_to` to a subsequent `apply_branch/2` — the single rollback path.
        Logger.warning(
          "REORG detected at height #{expected_height}: expected #{prev_hash}, have #{stored_hash} — refusing child (reconcile will supply rollback)"
        )

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
  @spec rollback_to(non_neg_integer()) :: :ok | {:error, term()}
  def rollback_to(height) do
    # Fail-closed contract (note-1069 B2): the rollback's several statements run in ONE
    # transaction and the result is checked, so a mid-rollback DB error rolls the whole
    # rollback back and is reported as `{:error, _}` — the caller must not then claim the
    # rollback (and `last_height`) succeeded.
    case Repo.transaction(fn -> do_rollback_to(height) end) do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  rescue
    e -> {:error, {:rollback_exception, Exception.message(e)}}
  end

  defp do_rollback_to(height) do
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

    :ok
  end

  defp get_last_processed_height do
    BlockProcessContext
    |> order_by([b], desc: b.height)
    |> limit(1)
    |> select([b], b.height)
    |> Repo.one() || 0
  end
end
