defmodule Athanor.Indexer.TransactionProcessorOutOfOrderTest do
  @moduledoc """
  Out-of-order STAS lineage reprocessing — port of
  `dxs-consigliere/src/Dxs.Consigliere/BackgroundTasks/StasAttributesMissingTransactions.cs`.

  When a transfer is indexed before its parent issuance, the indexer
  cannot validate lineage and must:

    * persist the unknown parent txid into
      `meta.metadata["missing_transactions"]`
    * set `meta.metadata["all_stas_inputs_known"] = false`
    * leave the output's `utxos.token_id` as `nil` (lineage uncertain)

  Once the parent arrives, the indexer must:

    * detect that this new tx is a missing parent for some waiter
    * re-run lineage computation on each waiter
    * flip the waiter's flags + tag its UTXO `token_id` correctly
    * cascade: if reprocessing a waiter clears ITS missing list and
      another tx was waiting on that waiter, repeat
  """

  use Athanor.DataCase, async: false

  alias Athanor.Indexer.TransactionProcessor
  alias Athanor.Repo
  alias Athanor.Schema.{MetaTransaction, Utxo}
  alias BSV.Tokens.Script.Stas3Builder

  setup do
    case Process.whereis(TransactionProcessor) do
      nil -> start_supervised!(TransactionProcessor)
      _ -> :ok
    end

    :ok
  end

  defp build_stas3_script(proto, owner) do
    {:ok, script} =
      Stas3Builder.build_stas3_locking_script(
        owner,
        proto,
        nil,
        false,
        %BSV.Tokens.ScriptFlags{},
        [],
        []
      )

    script
  end

  defp insert_p2pkh_source_utxo(pkh) do
    source_txid = :crypto.strong_rand_bytes(32)
    script_bin = pkh |> BSV.Script.p2pkh_lock() |> BSV.Script.to_binary()

    {:ok, _} =
      %Utxo{}
      |> Utxo.changeset(%{
        txid: source_txid,
        vout: 0,
        address: BSV.Base58.check_encode(pkh, 0x00),
        satoshis: 100_000,
        script_hex: Base.encode16(script_bin, case: :lower),
        is_spent: false
      })
      |> Repo.insert()

    source_txid
  end

  # Bitcoin display-order: reverse byte order of the internal SHA256d hash.
  # `BSV.Transaction.tx_id_hex/1` produces this, and the indexer stores
  # txid-strings in this form.
  defp display_hex(<<_::binary-size(32)>> = txid_binary) do
    txid_binary
    |> :binary.bin_to_list()
    |> Enum.reverse()
    |> :binary.list_to_bin()
    |> Base.encode16(case: :lower)
  end

  defp build_tx(source_txid, source_vout, output_script, satoshis \\ 1000) do
    %BSV.Transaction{
      version: 1,
      lock_time: 0,
      inputs: [
        %BSV.Transaction.Input{
          source_txid: source_txid,
          source_tx_out_index: source_vout,
          sequence_number: 0xFFFFFFFF,
          unlocking_script: %BSV.Script{chunks: []}
        }
      ],
      outputs: [%BSV.Transaction.Output{satoshis: satoshis, locking_script: output_script}]
    }
  end

  test "transfer indexed before its parent issuance: marked unknown, then reconciled when parent arrives" do
    issuer_pkh = :binary.copy(<<0xE1>>, 20)
    proto = issuer_pkh
    proto_hex = Base.encode16(proto, case: :lower)
    new_owner = :binary.copy(<<0xE2>>, 20)

    # ── Construct the issuance tx (NOT YET INDEXED) ────────────────
    issuance_source_txid = insert_p2pkh_source_utxo(issuer_pkh)
    issuance_script = build_stas3_script(proto, issuer_pkh)
    issuance_tx = build_tx(issuance_source_txid, 0, issuance_script)
    issuance_txid_bin = BSV.Transaction.txid_binary(issuance_tx)
    issuance_txid_hex = display_hex(issuance_txid_bin)

    # ── Index a transfer BEFORE its issuance parent ────────────────
    transfer_script = build_stas3_script(proto, new_owner)
    transfer_tx = build_tx(issuance_txid_bin, 0, transfer_script)
    transfer_txid_bin = BSV.Transaction.txid_binary(transfer_tx)

    # The transfer's input has unlocking_script == empty, but its
    # source_txid points at the unindexed issuance. `compute_stas_attributes`
    # must recognize this as an unknown STAS parent because the input's
    # source_txid resolves to nothing in our utxos table AND the output
    # is STAS-templated (so we're definitionally in the STAS lineage).
    {:ok, _} = TransactionProcessor.process_tx(transfer_tx, [], [])

    meta_before = Repo.get_by!(MetaTransaction, txid: transfer_txid_bin)
    assert meta_before.metadata["is_stas"] == true
    assert meta_before.metadata["all_stas_inputs_known"] == false

    assert issuance_txid_hex in (meta_before.metadata["missing_transactions"] || []),
           "transfer should record its missing parent issuance txid"

    transfer_utxo_before = Repo.get_by!(Utxo, txid: transfer_txid_bin, vout: 0)

    assert is_nil(transfer_utxo_before.token_id),
           "transfer output must not be tagged while lineage is unknown"

    # ── Now the parent issuance arrives ────────────────────────────
    {:ok, _} = TransactionProcessor.process_tx(issuance_tx, [], [])

    issuance_utxo = Repo.get_by!(Utxo, txid: issuance_txid_bin, vout: 0)
    assert issuance_utxo.token_id == proto_hex

    # ── The transfer must have been reprocessed ────────────────────
    meta_after = Repo.get_by!(MetaTransaction, txid: transfer_txid_bin)
    assert meta_after.metadata["all_stas_inputs_known"] == true
    assert meta_after.metadata["missing_transactions"] in [nil, []]

    transfer_utxo_after = Repo.get_by!(Utxo, txid: transfer_txid_bin, vout: 0)

    assert transfer_utxo_after.token_id == proto_hex,
           "transfer output should inherit the issuance's tag after its parent is indexed"
  end

  test "cascading reprocess: transfer-of-transfer when grand-parent arrives last" do
    # Chain: issuance → transfer A → transfer B
    # Insertion order: B, A, issuance (reverse). Final state must be:
    # all three flagged correctly with token_id == proto.

    issuer_pkh = :binary.copy(<<0xC1>>, 20)
    proto = issuer_pkh
    proto_hex = Base.encode16(proto, case: :lower)
    owner_a = :binary.copy(<<0xC2>>, 20)
    owner_b = :binary.copy(<<0xC3>>, 20)

    # Build (but don't index yet) the issuance tx
    issuance_source_txid = insert_p2pkh_source_utxo(issuer_pkh)
    issuance_script = build_stas3_script(proto, issuer_pkh)
    issuance_tx = build_tx(issuance_source_txid, 0, issuance_script)
    issuance_txid_bin = BSV.Transaction.txid_binary(issuance_tx)

    # Build transfer A — spends issuance output, mints for owner_a
    transfer_a_script = build_stas3_script(proto, owner_a)
    transfer_a_tx = build_tx(issuance_txid_bin, 0, transfer_a_script)
    transfer_a_txid_bin = BSV.Transaction.txid_binary(transfer_a_tx)

    # Build transfer B — spends A's output, mints for owner_b
    transfer_b_script = build_stas3_script(proto, owner_b)
    transfer_b_tx = build_tx(transfer_a_txid_bin, 0, transfer_b_script)
    transfer_b_txid_bin = BSV.Transaction.txid_binary(transfer_b_tx)

    # Index in REVERSE order: B → A → issuance
    {:ok, _} = TransactionProcessor.process_tx(transfer_b_tx, [], [])
    {:ok, _} = TransactionProcessor.process_tx(transfer_a_tx, [], [])
    {:ok, _} = TransactionProcessor.process_tx(issuance_tx, [], [])

    # All three should now resolve to the correct issuance set.
    for txid_bin <- [issuance_txid_bin, transfer_a_txid_bin, transfer_b_txid_bin] do
      meta = Repo.get_by!(MetaTransaction, txid: txid_bin)

      assert meta.metadata["all_stas_inputs_known"] == true,
             "after the issuance arrives, the full chain must clear its missing-parent flag"

      assert meta.metadata["missing_transactions"] in [nil, []]

      utxo = Repo.get_by!(Utxo, txid: txid_bin, vout: 0)
      assert utxo.token_id == proto_hex
    end
  end
end
