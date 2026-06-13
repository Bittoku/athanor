defmodule Athanor.Indexer.BlockProcessorReorgTest do
  @moduledoc """
  Reorg rollback behaviour for `BlockProcessor.rollback_to/1`.

  On a chain reorg the indexer demotes every transaction in an orphaned
  block back to the unconfirmed state — it does NOT delete them, so a
  re-mine simply re-confirms. The correctness fix this test pins:

    * a UTXO spent by an orphaned transaction is *freed*
      (`is_spent` → false, `spent_txid` → nil). Without this the UTXO
      set keeps a phantom spend for a transaction that may never
      reappear on the new chain.

  Transactions at or below the rollback height, and the UTXOs they own,
  are left untouched.
  """

  use Athanor.DataCase, async: false

  alias Athanor.Indexer.BlockProcessor
  alias Athanor.Repo
  alias Athanor.Schema.{BlockProcessContext, MetaTransaction, Utxo}

  defp meta_fixture(txid, block_height) do
    {:ok, m} =
      %MetaTransaction{}
      |> MetaTransaction.changeset(%{
        txid: txid,
        hex: "00",
        timestamp: System.os_time(:second),
        is_confirmed: true,
        block_height: block_height
      })
      |> Repo.insert()

    m
  end

  defp utxo_fixture(attrs) do
    {:ok, u} =
      %Utxo{}
      |> Utxo.changeset(
        Map.merge(
          %{
            txid: :crypto.strong_rand_bytes(32),
            vout: 0,
            address: "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa",
            satoshis: 1000,
            script_hex: "76a90088ac",
            is_spent: false
          },
          attrs
        )
      )
      |> Repo.insert()

    u
  end

  test "rollback frees UTXOs spent by orphaned transactions" do
    issuance_txid = :crypto.strong_rand_bytes(32)
    transfer_txid = :crypto.strong_rand_bytes(32)

    # Block 100: issuance F. Block 101: transfer T, which spends F's output.
    meta_fixture(issuance_txid, 100)
    meta_fixture(transfer_txid, 101)

    # F's output — confirmed at 100, spent by the (orphaned) transfer T.
    f_output =
      utxo_fixture(%{
        txid: issuance_txid,
        vout: 0,
        block_height: 100,
        is_spent: true,
        spent_txid: transfer_txid
      })

    # T's own output — confirmed at 101 (orphaned block).
    t_output = utxo_fixture(%{txid: transfer_txid, vout: 0, block_height: 101})

    {:ok, _} =
      %BlockProcessContext{}
      |> BlockProcessContext.changeset(%{
        id: "block-101",
        height: 101,
        processed_at: DateTime.utc_now() |> DateTime.truncate(:second)
      })
      |> Repo.insert()

    # Reorg: roll back to height 100 — block 101 is orphaned.
    BlockProcessor.rollback_to(100)

    # F's output must be FREED — the transfer that spent it is orphaned.
    freed = Repo.get!(Utxo, f_output.id)
    assert freed.is_spent == false
    assert is_nil(freed.spent_txid)
    # F itself is at height 100, not orphaned — stays confirmed.
    assert freed.block_height == 100

    # The transfer is demoted to unconfirmed, not deleted.
    transfer_meta = Repo.get_by!(MetaTransaction, txid: transfer_txid)
    assert transfer_meta.is_confirmed == false
    assert is_nil(transfer_meta.block_height)

    # T's output is un-confirmed but still present.
    t_after = Repo.get!(Utxo, t_output.id)
    assert is_nil(t_after.block_height)

    # The issuance at height 100 is untouched.
    issuance_meta = Repo.get_by!(MetaTransaction, txid: issuance_txid)
    assert issuance_meta.is_confirmed == true
    assert issuance_meta.block_height == 100

    # The orphaned block context is gone.
    assert is_nil(Repo.get(BlockProcessContext, "block-101"))
  end

  test "apply_reorg/3 rolls back through the mailbox, after queued block work (serialized)" do
    # Blocker 3 (Hermes !18 note 932): the P2P reorg must not run rollback in the
    # caller while old-branch block casts are queued/in-flight. Routed as a single
    # `{:apply_reorg, fork, connect}` cast, rollback is serialized behind any
    # already-queued `process_block_hash` work in the BlockProcessor mailbox.
    proc = start_supervised!(BlockProcessor)

    transfer_txid = :crypto.strong_rand_bytes(32)
    meta_fixture(transfer_txid, 101)
    t_output = utxo_fixture(%{txid: transfer_txid, vout: 0, block_height: 101})

    {:ok, _} =
      %BlockProcessContext{}
      |> BlockProcessContext.changeset(%{
        id: "block-101",
        height: 101,
        processed_at: DateTime.utc_now() |> DateTime.truncate(:second)
      })
      |> Repo.insert()

    # Queue a stale old-branch block first (RPC is down in test → it is a no-op),
    # THEN the reorg. FIFO ordering means the reorg's rollback runs after the
    # queued cast — never concurrently with it.
    GenServer.cast(proc, {:process_block_hash, :crypto.strong_rand_bytes(32)})
    :ok = BlockProcessor.apply_reorg(100, [])
    # Drain the mailbox: the stale cast, then apply_reorg.
    _ = :sys.get_state(proc)

    # Block 101 was orphaned by the rollback embedded in the reorg op.
    assert is_nil(Repo.get(BlockProcessContext, "block-101"))
    demoted = Repo.get_by!(MetaTransaction, txid: transfer_txid)
    assert demoted.is_confirmed == false
    assert is_nil(demoted.block_height)
    assert is_nil(Repo.get!(Utxo, t_output.id).block_height)
  end

  test "apply_reorg(fork, []) advances last_height to the fork height after rollback (note 941 B3)" do
    proc = start_supervised!(BlockProcessor)

    {:ok, _} =
      %BlockProcessContext{}
      |> BlockProcessContext.changeset(%{
        id: "block-101",
        height: 101,
        processed_at: DateTime.utc_now() |> DateTime.truncate(:second)
      })
      |> Repo.insert()

    # Rollback to 100 with no canonical branch to connect (node at the ancestor).
    :ok = BlockProcessor.apply_reorg(100, [])
    _ = :sys.get_state(proc)

    # The DB was rolled back; the GenServer must NOT still advertise the orphaned
    # tip height — it reflects the fork height.
    assert is_nil(Repo.get(BlockProcessContext, "block-101"))
    assert BlockProcessor.last_processed_height() == 100
  end

  test "apply_reorg(fork, [failing_hash]) keeps last_height at the fork when the connect block fails (note 941 B3)" do
    proc = start_supervised!(BlockProcessor)

    {:ok, _} =
      %BlockProcessContext{}
      |> BlockProcessContext.changeset(%{
        id: "block-101",
        height: 101,
        processed_at: DateTime.utc_now() |> DateTime.truncate(:second)
      })
      |> Repo.insert()

    # The connect block can't be fetched (RPC stub errors), so it must not advance
    # last_height past the fork — never leave it at the stale orphaned height.
    :ok = BlockProcessor.apply_reorg(100, [:crypto.strong_rand_bytes(32)])
    _ = :sys.get_state(proc)

    assert BlockProcessor.last_processed_height() == 100
  end

  # Hermes !18 note 945 B3: no-gap connect + predecessor guard.
  test "connect_branch halts at the first failed connect and does not process later blocks" do
    state = %{last_height: 100, processing: false}

    process_fun = fn
      <<0xAA, _::binary>>, _acc ->
        {:ok, 101}

      <<0xBB, _::binary>>, _acc ->
        {:error, :boom}

      <<0xCC, _::binary>>, _acc ->
        flunk("must not process a block after an earlier connect failed")
    end

    result =
      BlockProcessor.connect_branch(
        [
          <<0xAA, 0::248>>,
          <<0xBB, 0::248>>,
          <<0xCC, 0::248>>
        ],
        state,
        process_fun
      )

    # Advanced to the last contiguous success (101), then halted before 0xCC.
    assert result.last_height == 101
  end

  test "connect_branch with a failing first block keeps last_height unchanged" do
    state = %{last_height: 100, processing: false}

    process_fun = fn
      <<0xFA, _::binary>>, _acc ->
        {:error, :boom}

      <<0x5C, _::binary>>, _acc ->
        flunk("succeeding block must not run after the earlier failure")
    end

    result =
      BlockProcessor.connect_branch([<<0xFA, 0::248>>, <<0x5C, 0::248>>], state, process_fun)

    assert result.last_height == 100
  end

  test "maybe_handle_reorg refuses a non-genesis block whose predecessor context is missing" do
    {:ok, _} =
      %BlockProcessContext{}
      |> BlockProcessContext.changeset(%{
        id: "h100",
        height: 100,
        processed_at: DateTime.utc_now() |> DateTime.truncate(:second)
      })
      |> Repo.insert()

    # A block at 105 (predecessor 104 absent) while the index is non-empty → gap.
    assert {:error, :missing_predecessor} = BlockProcessor.maybe_handle_reorg("h104", 105)
  end

  test "maybe_handle_reorg allows the first block of an empty index (no predecessor expected)" do
    assert :ok = BlockProcessor.maybe_handle_reorg("genesis_prev", 1)
  end

  test "maybe_handle_reorg is :ok when the predecessor context matches" do
    {:ok, _} =
      %BlockProcessContext{}
      |> BlockProcessContext.changeset(%{
        id: "h100",
        height: 100,
        processed_at: DateTime.utc_now() |> DateTime.truncate(:second)
      })
      |> Repo.insert()

    assert :ok = BlockProcessor.maybe_handle_reorg("h100", 101)
  end

  test "maybe_handle_reorg rolls back (and is :ok) on a predecessor hash mismatch" do
    {:ok, _} =
      %BlockProcessContext{}
      |> BlockProcessContext.changeset(%{
        id: "h100",
        height: 100,
        processed_at: DateTime.utc_now() |> DateTime.truncate(:second)
      })
      |> Repo.insert()

    # Block at 101 claims a different parent than our stored height-100 context.
    assert :ok = BlockProcessor.maybe_handle_reorg("not-h100", 101)
    # Rolled back below 100 → the stored context is gone.
    assert is_nil(Repo.get(BlockProcessContext, "h100"))
  end

  test "rollback leaves a UTXO spent by a non-orphaned tx alone" do
    deep_txid = :crypto.strong_rand_bytes(32)
    spender_txid = :crypto.strong_rand_bytes(32)

    # Both transactions are at/below the rollback height — neither orphaned.
    meta_fixture(deep_txid, 50)
    meta_fixture(spender_txid, 60)

    spent =
      utxo_fixture(%{
        txid: deep_txid,
        vout: 0,
        block_height: 50,
        is_spent: true,
        spent_txid: spender_txid
      })

    BlockProcessor.rollback_to(100)

    # Nothing above height 100 — the spend must survive untouched.
    after_rollback = Repo.get!(Utxo, spent.id)
    assert after_rollback.is_spent == true
    assert after_rollback.spent_txid == spender_txid
  end
end
