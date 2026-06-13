defmodule Athanor.Indexer.BlockProcessorApplyBranchTest do
  @moduledoc """
  Phase 7 F7.2 (T7.1) — `BlockProcessor.apply_branch/2` (the single, synchronous,
  ordered index-mutation op with a result contract), the bootstrap predecessor
  guard (`predecessor_status/3`), and the removal of the public
  `{:process_block_hash, …}` mutation cast.

  `connect_branch/3` is exercised through an injected `process_fun` (no DB/RPC);
  the bootstrap guard and the removed-cast regression use `DataCase`.
  """
  use Athanor.DataCase, async: false

  alias Athanor.Indexer.{BlockProcessor, TransactionProcessor}
  alias Athanor.Schema.{AddressHistory, BlockProcessContext, MetaTransaction, Utxo}

  defp state(last_height), do: %{last_height: last_height, processing: false}

  describe "connect_branch/3 result contract" do
    test "{:ok, last_height} when every connect block applies" do
      fun = fn
        <<0xAA, _::binary>>, _ -> {:ok, 101}
        <<0xBB, _::binary>>, _ -> {:ok, 102}
      end

      {result, st} =
        BlockProcessor.connect_branch([<<0xAA, 0::248>>, <<0xBB, 0::248>>], state(100),
          process_fun: fun
        )

      assert result == {:ok, 102}
      assert st.last_height == 102
    end

    test "{:partial, last_height, reason} halts at the first failure after progress" do
      fun = fn
        <<0xAA, _::binary>>, _ -> {:ok, 101}
        <<0xBB, _::binary>>, _ -> {:error, :boom}
        <<0xCC, _::binary>>, _ -> flunk("must not process a block after an earlier failure")
      end

      {result, st} =
        BlockProcessor.connect_branch(
          [<<0xAA, 0::248>>, <<0xBB, 0::248>>, <<0xCC, 0::248>>],
          state(100),
          process_fun: fun
        )

      assert result == {:partial, 101, :boom}
      assert st.last_height == 101
    end

    test "{:partial, …} when nothing connected but a rollback already mutated (mutated?: true)" do
      fun = fn _, _ -> {:error, :down} end

      {result, st} =
        BlockProcessor.connect_branch([<<0xFA, 0::248>>], state(100),
          process_fun: fun,
          mutated?: true
        )

      assert result == {:partial, 100, :down}
      assert st.last_height == 100
    end

    test "{:error, reason} when the first connect fails and nothing was mutated" do
      fun = fn _, _ -> {:error, :down} end

      {result, _st} =
        BlockProcessor.connect_branch([<<0xFA, 0::248>>], state(100), process_fun: fun)

      assert result == {:error, :down}
    end

    test "{:ok, last_height} for an empty connect (no-op)" do
      {result, st} = BlockProcessor.connect_branch([], state(100))
      assert result == {:ok, 100}
      assert st.last_height == 100
    end
  end

  describe "predecessor_status/3 (bootstrap boundary)" do
    test ":ok when the predecessor context matches" do
      Repo.insert!(%BlockProcessContext{id: "h100", height: 100, processed_at: now()})
      assert :ok = BlockProcessor.predecessor_status("h100", 101, bootstrap: nil)
    end

    test "refuses the child on a predecessor mismatch WITHOUT mutating (note-1061 B2)" do
      Repo.insert!(%BlockProcessContext{id: "h100", height: 100, processed_at: now()})

      assert {:error, :missing_predecessor} =
               BlockProcessor.predecessor_status("x", 101, bootstrap: nil)

      # The guard is read-only: it must NOT roll back here. `{:error, _}` means nothing
      # was mutated; the divergent predecessor is corrected by the reconcile-supplied
      # `rollback_to` in a subsequent `apply_branch/2`, the single rollback authority.
      refute is_nil(Repo.get(BlockProcessContext, "h100"))
    end

    test "accepts a missing-predecessor block ONLY when it is the configured bootstrap block" do
      # bootstrap height 200, hash unconstrained.
      assert :ok =
               BlockProcessor.predecessor_status("anything", 200,
                 bootstrap: %{height: 200, hash: nil}
               )
    end

    test "refuses an arbitrary high block on an empty index (no bootstrap match)" do
      assert {:error, :missing_predecessor} =
               BlockProcessor.predecessor_status("h105prev", 105,
                 bootstrap: %{height: 200, hash: nil}
               )
    end

    test "with a hash-pinned bootstrap, the bootstrap block must also match the hash" do
      bs = %{height: 200, hash: "BOOT"}

      assert :ok =
               BlockProcessor.predecessor_status("ignored", 200,
                 bootstrap: bs,
                 block_hash: "BOOT"
               )

      assert {:error, :missing_predecessor} =
               BlockProcessor.predecessor_status("ignored", 200,
                 bootstrap: bs,
                 block_hash: "OTHER"
               )
    end

    test "refuses a missing-predecessor block when no bootstrap is configured" do
      assert {:error, :missing_predecessor} =
               BlockProcessor.predecessor_status("h105prev", 105, bootstrap: nil)
    end
  end

  describe "apply_branch/2 (synchronous, ordered)" do
    test "rollback with an empty connect returns {:ok, fork} and drops last_height to the fork" do
      proc = start_supervised!(BlockProcessor)
      Repo.insert!(%BlockProcessContext{id: "block-101", height: 101, processed_at: now()})

      assert {:ok, 100} = BlockProcessor.apply_branch(proc, %{rollback_to: 100, connect: []})
      assert is_nil(Repo.get(BlockProcessContext, "block-101"))
      assert BlockProcessor.last_processed_height() == 100
    end

    test "rollback then a failing connect returns {:partial, fork, _} and keeps last_height at the fork" do
      proc = start_supervised!(BlockProcessor)
      Repo.insert!(%BlockProcessContext{id: "block-101", height: 101, processed_at: now()})

      # The connect block can't be fetched (RPC stub errors) → partial, last_height stays 100.
      assert {:partial, 100, _reason} =
               BlockProcessor.apply_branch(proc, %{
                 rollback_to: 100,
                 connect: [:crypto.strong_rand_bytes(32)]
               })

      assert BlockProcessor.last_processed_height() == 100
    end

    test "waits for a slow ordered mutation and returns its synchronous result (note-1061 B1)" do
      # An injected processor that blocks until released, simulating a slow RPC/DB/batch
      # path. The call must wait for the in-flight ordered mutation and return its real
      # result — never let a call timeout fold a false failure while the server keeps
      # mutating. `apply_branch/2` uses `:infinity` (bounded batches), so a slow op is
      # carried to completion synchronously.
      test = self()

      blocker = fn _hash, state ->
        send(test, :in_mutation)

        receive do
          :proceed -> :ok
        end

        {:ok, state.last_height + 1}
      end

      proc = start_supervised!({BlockProcessor, process_fun: blocker})

      task =
        Task.async(fn ->
          BlockProcessor.apply_branch(proc, %{rollback_to: nil, connect: [<<1, 0::248>>]})
        end)

      # The mutation is in flight inside the server, blocked until released.
      assert_receive :in_mutation, 1_000
      # The call has not returned/exited — it is waiting on the mutation, not folding.
      refute Task.yield(task, 50)

      send(proc, :proceed)
      assert {:ok, 1} = Task.await(task, 1_000)
      assert BlockProcessor.last_processed_height() == 1
    end

    test "a first-block predecessor refusal returns {:error}, last_height unchanged, no rollback (note-1061 B2)" do
      # Seed a durable tip at 100, then refuse the first connect block (as the read-only
      # predecessor guard does on a mismatch). The contract must hold: {:error} (nothing
      # mutated), last_height stays at the durable tip, and the existing context is kept.
      Repo.insert!(%BlockProcessContext{id: "h100", height: 100, processed_at: now()})
      refuse = fn _hash, _state -> {:error, :missing_predecessor} end
      proc = start_supervised!({BlockProcessor, process_fun: refuse})

      assert {:error, :missing_predecessor} =
               BlockProcessor.apply_branch(proc, %{rollback_to: nil, connect: [<<9, 0::248>>]})

      assert BlockProcessor.last_processed_height() == 100
      refute is_nil(Repo.get(BlockProcessContext, "h100"))
    end
  end

  describe "the public {:process_block_hash, …} mutation cast is removed (note-1035 B1)" do
    test "a direct GenServer.cast cannot create or update block_process_contexts" do
      proc = start_supervised!(BlockProcessor)

      GenServer.cast(proc, {:process_block_hash, :crypto.strong_rand_bytes(32)})
      _ = :sys.get_state(proc)

      # No context was written — the only mutation path is apply_branch/2.
      assert Repo.aggregate(BlockProcessContext, :count) == 0
    end
  end

  describe "record_block/3 is atomic with the durable context (note-1057)" do
    test "a failed context insert returns {:error, _} and records no context for the block" do
      # A different block already occupies height 150 → the unique-height constraint
      # makes the new block's context insert fail. The processor must NOT report the
      # block applied: it returns {:error, _} so connect_branch halts and the
      # controller defers, and no context row is written for the new block.
      Repo.insert!(%BlockProcessContext{id: "other-150", height: 150, processed_at: now()})

      assert {:error, {:context_insert_failed, _}} =
               BlockProcessor.record_block("new-150", 150, [])

      assert is_nil(Repo.get(BlockProcessContext, "new-150"))
      assert Repo.aggregate(BlockProcessContext, :count) == 1
    end

    test "per-block confirmation side effects roll back when the context insert fails" do
      txid = :crypto.strong_rand_bytes(32)
      txid_hex = Base.encode16(txid, case: :lower)
      # A real (valid-hex) block hash so the confirmation path decodes it.
      block_hash_hex = String.duplicate("ab", 32)

      {:ok, meta} =
        %MetaTransaction{}
        |> MetaTransaction.changeset(%{txid: txid, hex: "00", timestamp: 0, is_confirmed: false})
        |> Repo.insert()

      # Same height already taken → the in-transaction context insert fails after the
      # confirmation update; that update must roll back with it (no side effect commits
      # without the durable context row).
      Repo.insert!(%BlockProcessContext{id: "other-160", height: 160, processed_at: now()})

      assert {:error, {:context_insert_failed, _}} =
               BlockProcessor.record_block(block_hash_hex, 160, [txid_hex])

      refute Repo.get(MetaTransaction, meta.id).is_confirmed
    end

    test "a fresh block records its context and returns {:ok, height}" do
      assert {:ok, 170} = BlockProcessor.record_block("block-170", 170, [])
      assert Repo.get(BlockProcessContext, "block-170").height == 170
    end
  end

  # A matched P2PKH tx fixture (same shape the TransactionProcessor tests use): one
  # P2PKH output to `pkh`, so `index_tx/4` writes a MetaTransaction, a UTXO, and an
  # address-history row.
  defp p2pkh_tx(pkh) do
    %BSV.Transaction{
      version: 1,
      inputs: [
        %BSV.Transaction.Input{
          source_txid: :crypto.strong_rand_bytes(32),
          source_tx_out_index: 0,
          unlocking_script: %BSV.Script{chunks: []},
          sequence_number: 0xFFFFFFFF
        }
      ],
      outputs: [
        %BSV.Transaction.Output{satoshis: 1000, locking_script: BSV.Script.p2pkh_lock(pkh)}
      ],
      lock_time: 0
    }
  end

  describe "record_block/4 is atomic with the matched-tx side effects (note-1065)" do
    test "a failed context insert rolls back the matched block-tx meta/UTXO/address-history" do
      pkh = :binary.copy(<<0x55>>, 20)
      address = BSV.Base58.check_encode(pkh, 0x00)
      tx = p2pkh_tx(pkh)

      # Occupy height 300 so the block-context insert conflicts and forces a rollback.
      Repo.insert!(%BlockProcessContext{id: "other-300", height: 300, processed_at: now()})

      # The REAL matched-tx indexing (inline `index_tx/4`, no RPC), running inside
      # record_block's transaction alongside the failing context insert.
      index_fun = fn _tx_data, _h, _bh ->
        case TransactionProcessor.index_tx(tx, [address], [], :block) do
          {:ok, _} -> :ok
          err -> err
        end
      end

      assert {:error, {:context_insert_failed, _}} =
               BlockProcessor.record_block("new-300", 300, ["t"], index_fun: index_fun)

      # No side effects survived without a durable context row.
      assert Repo.aggregate(MetaTransaction, :count) == 0
      assert Repo.aggregate(Utxo, :count) == 0
      assert Repo.aggregate(AddressHistory, :count) == 0
      assert is_nil(Repo.get(BlockProcessContext, "new-300"))
    end

    test "a successful record commits the matched-tx side effects together with the context" do
      pkh = :binary.copy(<<0x56>>, 20)
      address = BSV.Base58.check_encode(pkh, 0x00)
      tx = p2pkh_tx(pkh)

      index_fun = fn _tx_data, _h, _bh ->
        case TransactionProcessor.index_tx(tx, [address], [], :block) do
          {:ok, _} -> :ok
          err -> err
        end
      end

      assert {:ok, 301} =
               BlockProcessor.record_block("blk-301", 301, ["t"], index_fun: index_fun)

      assert Repo.get(BlockProcessContext, "blk-301").height == 301
      assert Repo.aggregate(MetaTransaction, :count) == 1
      assert Repo.aggregate(Utxo, :count) == 1
    end

    test "a tx-indexing error fails the block closed (no context, no partial side effects)" do
      # The injected indexer reports a failure → record_block must roll back and return
      # {:error, {:tx_index_failed, _}} without recording the context.
      index_fun = fn _tx_data, _h, _bh -> {:error, :boom} end

      assert {:error, {:tx_index_failed, :boom}} =
               BlockProcessor.record_block("blk-302", 302, ["t"], index_fun: index_fun)

      assert is_nil(Repo.get(BlockProcessContext, "blk-302"))
    end
  end

  describe "block-indexing writes are checked + classified (note-1069 B1)" do
    test "required_write/1: success and benign :not_found pass; a real error fails closed" do
      # The gate that ALL block-indexing writes (spend_utxo/set_stas3_op/create_utxo/
      # union_source_only/AddressHistory) pass through: a missing watched UTXO is benign,
      # any other error fails the block closed.
      assert :ok = TransactionProcessor.required_write({:ok, :whatever})
      assert :ok = TransactionProcessor.required_write({:error, :not_found})
      assert {:error, :db_down} = TransactionProcessor.required_write({:error, :db_down})

      assert {:error, %Ecto.Changeset{}} =
               TransactionProcessor.required_write({:error, %Ecto.Changeset{}})
    end

    test "a matched tx that spends an UNWATCHED parent still indexes (benign :not_found)" do
      # The tx's input spends a random (unwatched) UTXO → spend_utxo returns
      # {:error, :not_found}, which must NOT fail the block: the tx indexes fully and
      # record_block/4 commits both the side effects and the context.
      pkh = :binary.copy(<<0x57>>, 20)
      address = BSV.Base58.check_encode(pkh, 0x00)
      tx = p2pkh_tx(pkh)

      index_fun = fn _t, _h, _bh ->
        case TransactionProcessor.index_tx(tx, [address], [], :block) do
          {:ok, _} -> :ok
          err -> err
        end
      end

      assert {:ok, 305} =
               BlockProcessor.record_block("blk-305", 305, ["t"], index_fun: index_fun)

      assert Repo.aggregate(MetaTransaction, :count) == 1
      assert Repo.aggregate(Utxo, :count) == 1
      assert Repo.get(BlockProcessContext, "blk-305").height == 305
    end
  end

  describe "rollback is fail-closed and result-checked (note-1069 B2)" do
    test "a failed rollback returns {:error}, leaves last_height unchanged, and skips connect" do
      Repo.insert!(%BlockProcessContext{id: "h100", height: 100, processed_at: now()})

      proc =
        start_supervised!({
          BlockProcessor,
          rollback_fun: fn _height -> {:error, :db_down} end,
          process_fun: fn _hash, _state ->
            flunk("connect must not run after a failed rollback")
          end
        })

      assert {:error, {:rollback_failed, :db_down}} =
               BlockProcessor.apply_branch(proc, %{rollback_to: 50, connect: [<<1, 0::248>>]})

      # The durable tip is untouched and `last_height` was not lowered to the fork.
      assert BlockProcessor.last_processed_height() == 100
      refute is_nil(Repo.get(BlockProcessContext, "h100"))
    end

    test "rollback_to/1 runs in one transaction and returns :ok" do
      Repo.insert!(%BlockProcessContext{id: "h100", height: 100, processed_at: now()})
      Repo.insert!(%BlockProcessContext{id: "h101", height: 101, processed_at: now()})

      assert :ok = BlockProcessor.rollback_to(100)

      assert is_nil(Repo.get(BlockProcessContext, "h101"))
      refute is_nil(Repo.get(BlockProcessContext, "h100"))
    end
  end

  defp now, do: DateTime.utc_now() |> DateTime.truncate(:second)
end
