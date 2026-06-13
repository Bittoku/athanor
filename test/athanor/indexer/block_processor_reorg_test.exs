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
