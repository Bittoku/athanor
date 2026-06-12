defmodule Athanor.P2P.MempoolObserver.DedupeTest do
  @moduledoc """
  Cross-source dedupe (Phase 3 T3.5 / §A end-to-end against the store): the same
  watched txid observed via the **P2P** path (`source: :p2p`, what the
  `MempoolObserver` pipeline emits) and via an **existing** path (`source:
  :junglebus`, what `TransactionFilter.process_raw_tx/2` emits) must be indexed
  **once** — one `MetaTransaction` row, no duplicate address-history rows — with
  `metadata["sources"]` containing **both**, order-independently.

  Exercised at the store boundary via `TransactionProcessor.process_tx/4` (the
  shared sink both ingress paths converge on); the live P2P delivery itself is
  covered by `integration_test.exs`.
  """
  use Athanor.DataCase, async: false

  import Ecto.Query

  alias Athanor.Indexer.TransactionProcessor
  alias Athanor.Repo
  alias Athanor.Schema.{AddressHistory, MetaTransaction}
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

  defp meta(tx), do: Repo.get_by(MetaTransaction, txid: Transaction.txid_binary(tx))

  defp meta_count(tx),
    do:
      Repo.aggregate(
        from(m in MetaTransaction, where: m.txid == ^Transaction.txid_binary(tx)),
        :count
      )

  defp history_count(tx),
    do:
      Repo.aggregate(
        from(h in AddressHistory, where: h.txid == ^Transaction.tx_id_hex(tx)),
        :count
      )

  test "a txid seen via :p2p and via the existing path is indexed once with both sources" do
    pkh = :binary.copy(<<0x61>>, 20)
    address = BSV.Base58.check_encode(pkh, 0x00)
    tx = p2pkh_tx(pkh)

    # P2P path (observer pipeline) first, then the existing JungleBus path.
    TransactionProcessor.process_tx(tx, [address], [], :p2p)
    TransactionProcessor.process_tx(tx, [address], [], :junglebus)

    assert meta_count(tx) == 1
    assert history_count(tx) == 1
    assert Enum.sort(meta(tx).metadata["sources"]) == ["junglebus", "p2p"]
  end

  test "the union is order-independent (existing path first, then :p2p)" do
    pkh = :binary.copy(<<0x62>>, 20)
    address = BSV.Base58.check_encode(pkh, 0x00)
    tx = p2pkh_tx(pkh)

    TransactionProcessor.process_tx(tx, [address], [], :junglebus)
    TransactionProcessor.process_tx(tx, [address], [], :p2p)
    # A duplicate of an already-recorded source must not grow the set.
    TransactionProcessor.process_tx(tx, [address], [], :p2p)

    assert meta_count(tx) == 1
    assert history_count(tx) == 1
    assert Enum.sort(meta(tx).metadata["sources"]) == ["junglebus", "p2p"]
  end
end
