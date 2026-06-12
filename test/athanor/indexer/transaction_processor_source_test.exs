defmodule Athanor.Indexer.TransactionProcessorSourceTest do
  @moduledoc """
  Source-tagging + dedupe contract (plan §2.5 / Phase 3 T3.1): a tx's observing
  `source` is threaded through `process_tx/4` and recorded in
  `MetaTransaction.metadata["sources"]` as a set. First-seen wins for indexing;
  later observations only union their source (no second row, no double-index),
  order-independent. No schema migration.
  """
  use Athanor.DataCase, async: false

  import Ecto.Query

  alias Athanor.Indexer.TransactionProcessor
  alias Athanor.Repo
  alias Athanor.Schema.{AddressHistory, MetaTransaction, Utxo}
  alias BSV.Transaction

  setup do
    case Process.whereis(TransactionProcessor) do
      nil -> start_supervised!(TransactionProcessor)
      _ -> :ok
    end

    :ok
  end

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

  defp meta_for(tx), do: Repo.get_by(MetaTransaction, txid: Transaction.txid_binary(tx))

  defp count_for(tx),
    do:
      Repo.aggregate(
        from(m in MetaTransaction, where: m.txid == ^Transaction.txid_binary(tx)),
        :count
      )

  defp history_count_for(tx),
    do:
      Repo.aggregate(
        from(h in AddressHistory, where: h.txid == ^Transaction.tx_id_hex(tx)),
        :count
      )

  defp utxo_count_for(tx),
    do:
      Repo.aggregate(
        from(u in Utxo, where: u.txid == ^Transaction.txid_binary(tx)),
        :count
      )

  test "records the observing source on first index" do
    pkh = :binary.copy(<<0x42>>, 20)
    address = BSV.Base58.check_encode(pkh, 0x00)
    tx = p2pkh_tx(pkh)

    TransactionProcessor.process_tx(tx, [address], [], :p2p)

    assert meta_for(tx).metadata["sources"] == ["p2p"]
  end

  test "defaults to :unknown when no source is given (back-compat arity)" do
    pkh = :binary.copy(<<0x44>>, 20)
    address = BSV.Base58.check_encode(pkh, 0x00)
    tx = p2pkh_tx(pkh)

    TransactionProcessor.process_tx(tx, [address], [])

    assert meta_for(tx).metadata["sources"] == ["unknown"]
  end

  test "unions sources across re-observations: one row, deduped, order-independent" do
    pkh = :binary.copy(<<0x43>>, 20)
    address = BSV.Base58.check_encode(pkh, 0x00)
    tx = p2pkh_tx(pkh)

    TransactionProcessor.process_tx(tx, [address], [], :p2p)
    TransactionProcessor.process_tx(tx, [address], [], :junglebus)
    # A duplicate source must not grow the set.
    TransactionProcessor.process_tx(tx, [address], [], :p2p)

    assert count_for(tx) == 1
    assert Enum.sort(meta_for(tx).metadata["sources"]) == ["junglebus", "p2p"]
  end

  # !11 review blocker 1: a re-observation must ONLY union the source — it must
  # not replay first-index side effects (no duplicate address-history rows, no
  # duplicate UTXOs) and must not clobber confirmation columns set out-of-band
  # by the block processor.
  test "re-observation unions source without replaying side effects or un-confirming" do
    pkh = :binary.copy(<<0x45>>, 20)
    address = BSV.Base58.check_encode(pkh, 0x00)
    tx = p2pkh_tx(pkh)

    TransactionProcessor.process_tx(tx, [address], [], :junglebus)

    history_after_first = history_count_for(tx)
    utxos_after_first = utxo_count_for(tx)
    assert history_after_first == 1
    assert utxos_after_first == 1

    # Simulate the block processor confirming the tx out-of-band.
    meta_for(tx)
    |> MetaTransaction.changeset(%{is_confirmed: true, block_height: 800_000})
    |> Repo.update!()

    # A later P2P re-observation of the same (now-confirmed) tx.
    TransactionProcessor.process_tx(tx, [address], [], :p2p)

    meta = meta_for(tx)
    # Source unioned, single row.
    assert count_for(tx) == 1
    assert Enum.sort(meta.metadata["sources"]) == ["junglebus", "p2p"]
    # No replayed side effects.
    assert history_count_for(tx) == history_after_first
    assert utxo_count_for(tx) == utxos_after_first
    # Confirmation preserved (not reset to unconfirmed/nil).
    assert meta.is_confirmed == true
    assert meta.block_height == 800_000
  end

  # !11 review blocker 2: lineage reprocessing (reindex_lineage via
  # reprocess_waiters) recomputes the five flags, but must PRESERVE unrelated
  # metadata keys — especially "sources" — rather than overwriting the whole map.
  test "sources survive lineage reindex when a missing parent arrives" do
    # A deferred waiter: a previously-indexed tx whose lineage was held pending a
    # missing parent. It already carries a source set. We register it directly so
    # the test doesn't depend on STAS script construction.
    waiter_pkh = :binary.copy(<<0x46>>, 20)
    waiter_tx = p2pkh_tx(waiter_pkh)
    waiter_hex = Transaction.to_hex(waiter_tx)

    # The parent whose arrival re-triggers the waiter's lineage.
    parent_pkh = :binary.copy(<<0x47>>, 20)
    parent_tx = p2pkh_tx(parent_pkh)
    parent_display = Transaction.tx_id_hex(parent_tx)

    %MetaTransaction{}
    |> MetaTransaction.changeset(%{
      txid: Transaction.txid_binary(waiter_tx),
      hex: waiter_hex,
      is_confirmed: false,
      timestamp: System.os_time(:second),
      metadata: %{
        "sources" => ["junglebus"],
        "all_stas_inputs_known" => false,
        "missing_transactions" => [parent_display]
      }
    })
    |> Repo.insert!()

    # Indexing the parent fires reprocess_waiters(parent_display) → reindex_lineage(waiter).
    TransactionProcessor.process_tx(parent_tx, [], [], :p2p)

    waiter_meta = meta_for(waiter_tx)
    assert waiter_meta.metadata["sources"] == ["junglebus"]
    # And the flags were genuinely recomputed (no longer deferred).
    assert waiter_meta.metadata["all_stas_inputs_known"] == true
  end
end
