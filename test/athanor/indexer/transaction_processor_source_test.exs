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
  alias Athanor.Schema.MetaTransaction
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
end
