defmodule Athanor.Indexer.TransactionProcessorStas3Test do
  @moduledoc """
  End-to-end indexer behaviour for STAS 3.0 (spec v0.1 §4 / §5.2.3 / §8.2):

    * M3 — first observation of a watched protoID seeds the canonical
      post-OP_RETURN bytes; a subsequent output with the same protoID but
      different post-OP_RETURN bytes is logged + skipped.
    * M4 — service-field authorities (freezeAuth / confiscateAuth) are
      lifted from the issuance frame onto the `WatchingToken` row when
      the corresponding flag bits are set.
  """

  use Athanor.DataCase, async: false

  import ExUnit.CaptureLog

  alias Athanor.Indexer.TransactionProcessor
  alias Athanor.Repo
  alias Athanor.Schema.{WatchingToken, Utxo}
  alias BSV.Tokens.Script.Stas3Builder

  setup do
    # The TransactionProcessor calls into UtxoManager + WatchingToken
    # against the SQL sandbox. Spawn the GenServer if it's not already up.
    case Process.whereis(TransactionProcessor) do
      nil -> start_supervised!(TransactionProcessor)
      _ -> :ok
    end

    :ok
  end

  defp build_stas3_script(opts) do
    owner = Keyword.get(opts, :owner, :binary.copy(<<0xAA>>, 20))
    proto = Keyword.fetch!(opts, :redemption)
    flags = Keyword.get(opts, :flags, %BSV.Tokens.ScriptFlags{})
    service_fields = Keyword.get(opts, :service_fields, [])

    {:ok, script} =
      Stas3Builder.build_stas3_locking_script(
        owner,
        proto,
        nil,
        false,
        flags,
        service_fields,
        []
      )

    script
  end

  defp build_tx_with_output(script) do
    %BSV.Transaction{
      version: 1,
      lock_time: 0,
      inputs: [
        %BSV.Transaction.Input{
          # Non-coinbase: source_txid is non-zero so the processor takes
          # the spend-utxo branch even though we don't have a matching
          # UTXO row (UtxoManager.spend_utxo silently no-ops on misses).
          source_txid: :binary.copy(<<0x11>>, 32),
          source_tx_out_index: 0,
          sequence_number: 0xFFFFFFFF,
          unlocking_script: %BSV.Script{chunks: []}
        }
      ],
      outputs: [%BSV.Transaction.Output{satoshis: 1000, locking_script: script}]
    }
  end

  test "first STAS 3.0 output for a watched protoID seeds canonical bytes + service authorities" do
    proto = :binary.copy(<<0xCD>>, 20)
    proto_hex = Base.encode16(proto, case: :lower)

    freeze_pkh = :binary.copy(<<0xF1>>, 20)
    conf_pkh = :binary.copy(<<0xC1>>, 20)

    # Admin pre-registers the watched token by protoID.
    {:ok, _} =
      %WatchingToken{}
      |> WatchingToken.changeset(%{token_id: proto_hex})
      |> Repo.insert()

    flags = %BSV.Tokens.ScriptFlags{freezable: true, confiscatable: true}

    script =
      build_stas3_script(
        redemption: proto,
        flags: flags,
        service_fields: [freeze_pkh, conf_pkh]
      )

    tx = build_tx_with_output(script)

    {:ok, _txid} = TransactionProcessor.process_tx(tx, [], [proto_hex])

    refreshed = Repo.get_by!(WatchingToken, token_id: proto_hex)

    assert refreshed.canonical_post_op_return != nil
    assert byte_size(refreshed.canonical_post_op_return) > 0
    assert refreshed.freeze_auth == freeze_pkh
    assert refreshed.confiscate_auth == conf_pkh
  end

  test "tampered post-OP_RETURN payload is logged + skipped" do
    proto = :binary.copy(<<0xDE>>, 20)
    proto_hex = Base.encode16(proto, case: :lower)

    # Pre-seed the canonical bytes with a stub value that won't match
    # what the new output produces — emulating a previously-valid issuance
    # whose canonical region differs from the incoming malformed tx.
    canonical_stub = :crypto.strong_rand_bytes(64)

    {:ok, _} =
      %WatchingToken{}
      |> WatchingToken.changeset(%{
        token_id: proto_hex,
        canonical_post_op_return: canonical_stub
      })
      |> Repo.insert()

    script = build_stas3_script(redemption: proto)
    tx = build_tx_with_output(script)

    log =
      capture_log(fn ->
        {:ok, _txid} = TransactionProcessor.process_tx(tx, [], [proto_hex])
      end)

    assert log =~ "STAS3 post-OP_RETURN mismatch"

    # The output must NOT be indexed (canonical bytes diverge — spec §4
    # byte-identity invariant).
    txid_bin = BSV.Transaction.txid_binary(tx)
    assert nil == Repo.get_by(Utxo, txid: txid_bin, vout: 0)
  end

  describe "STAS 3.0 UTXO table indexing (regression for the address-field bug)" do
    # Prior to the fix landed alongside these tests, STAS3 outputs were
    # silently dropped from the `utxos` table because `BSV.Script.Address.from_script/2`
    # only recognises the P2PKH template — STAS3 scripts start with
    # `OP_PUSHDATA <20-byte owner PKH>` and don't match. The indexer set
    # `address = nil`, the Utxo changeset rejected the insert via
    # `validate_required(:address)`, and `UtxoManager.create_utxo/1`
    # discarded the error. Result: zero STAS3 rows ever indexed, every
    # downstream STAS3 query (balances, list_unspent, list_token_utxos,
    # stas3_op tagging, stas_attributes_observer) silently returned empty.
    #
    # These tests pin the fix: outputs reach the table, addresses are
    # derived from the owner PKH (spec v0.1 §5.2.2), spends flip
    # `is_spent`, and STAS3 receives are logged with the correct satoshis
    # value in `address_histories`.

    test "STAS3 output is inserted into utxos with an owner-derived base58 address" do
      owner_pkh = :binary.copy(<<0x7E>>, 20)
      expected_address = BSV.Base58.check_encode(owner_pkh, 0x00)

      # protoID must equal HASH160(Vin[0]'s spent output) for this to be
      # a valid issuance — see `compute_stas_attributes/2` in
      # `transaction_processor.ex`. We seed a P2PKH funding UTXO whose
      # address decodes to the same 20-byte hash that the STAS3 script
      # carries as its protoID.
      proto = :binary.copy(<<0x77>>, 20)
      issuer_address = BSV.Base58.check_encode(proto, 0x00)
      source_txid = :crypto.strong_rand_bytes(32)
      script_bin = proto |> BSV.Script.p2pkh_lock() |> BSV.Script.to_binary()

      {:ok, _} =
        %Utxo{}
        |> Utxo.changeset(%{
          txid: source_txid,
          vout: 0,
          address: issuer_address,
          satoshis: 100_000,
          script_hex: Base.encode16(script_bin, case: :lower),
          is_spent: false
        })
        |> Repo.insert()

      script = build_stas3_script(redemption: proto, owner: owner_pkh)

      tx = %BSV.Transaction{
        version: 1,
        lock_time: 0,
        inputs: [
          %BSV.Transaction.Input{
            source_txid: source_txid,
            source_tx_out_index: 0,
            sequence_number: 0xFFFFFFFF,
            unlocking_script: %BSV.Script{chunks: []}
          }
        ],
        outputs: [%BSV.Transaction.Output{satoshis: 1000, locking_script: script}]
      }

      {:ok, _txid} = TransactionProcessor.process_tx(tx, [], [])

      txid_bin = BSV.Transaction.txid_binary(tx)
      utxo = Repo.get_by(Utxo, txid: txid_bin, vout: 0)

      assert utxo != nil, "STAS3 output must land in the utxos table"
      assert utxo.address == expected_address
      assert utxo.token_type == "stas3"
      assert utxo.token_id == Base.encode16(proto, case: :lower)
      assert utxo.satoshis == 1000
      assert utxo.is_spent == false
    end

    test "spending a STAS3 UTXO flips is_spent=true on the parent row" do
      proto = :binary.copy(<<0x88>>, 20)
      parent_owner = :binary.copy(<<0x33>>, 20)

      parent_script = build_stas3_script(redemption: proto, owner: parent_owner)
      parent_tx = build_tx_with_output(parent_script)

      {:ok, _} = TransactionProcessor.process_tx(parent_tx, [], [])
      parent_txid_bin = BSV.Transaction.txid_binary(parent_tx)

      # Sanity: parent UTXO indexed and unspent.
      parent_utxo = Repo.get_by!(Utxo, txid: parent_txid_bin, vout: 0)
      refute parent_utxo.is_spent

      # Build a child tx that spends Vin[0] from the parent's STAS3 output
      # and re-mints a STAS3 output for a new owner. The unlocking script
      # is left empty — the indexer doesn't validate signatures.
      new_owner = :binary.copy(<<0x44>>, 20)
      child_script = build_stas3_script(redemption: proto, owner: new_owner)

      child_tx = %BSV.Transaction{
        version: 1,
        lock_time: 0,
        inputs: [
          %BSV.Transaction.Input{
            source_txid: parent_txid_bin,
            source_tx_out_index: 0,
            sequence_number: 0xFFFFFFFF,
            unlocking_script: %BSV.Script{chunks: []}
          }
        ],
        outputs: [
          %BSV.Transaction.Output{satoshis: 1000, locking_script: child_script}
        ]
      }

      {:ok, _} = TransactionProcessor.process_tx(child_tx, [], [])
      child_txid_bin = BSV.Transaction.txid_binary(child_tx)

      refreshed_parent = Repo.get_by!(Utxo, txid: parent_txid_bin, vout: 0)
      assert refreshed_parent.is_spent == true
      assert refreshed_parent.spent_txid == child_txid_bin

      # And the child is its own indexed row.
      child_utxo = Repo.get_by!(Utxo, txid: child_txid_bin, vout: 0)
      assert child_utxo.token_type == "stas3"
      assert child_utxo.address == BSV.Base58.check_encode(new_owner, 0x00)
    end

    test "STAS3 receive logs the correct satoshis in address_history" do
      # The pre-fix `calculate_address_amount/3` filtered outputs through
      # `BSV.Script.Address.from_script/2`, which returns `:error` for
      # STAS3 scripts — so every STAS3 receive was logged as
      # `satoshis: 0`. With `skip_zero_balance` on the wallet channel
      # filter, those rows would be hidden from clients.
      owner_pkh = :binary.copy(<<0x55>>, 20)
      address = BSV.Base58.check_encode(owner_pkh, 0x00)
      proto = :binary.copy(<<0x99>>, 20)
      satoshis = 4_242

      script = build_stas3_script(redemption: proto, owner: owner_pkh)

      tx = %BSV.Transaction{
        version: 1,
        lock_time: 0,
        inputs: [
          %BSV.Transaction.Input{
            source_txid: :binary.copy(<<0x11>>, 32),
            source_tx_out_index: 0,
            sequence_number: 0xFFFFFFFF,
            unlocking_script: %BSV.Script{chunks: []}
          }
        ],
        outputs: [%BSV.Transaction.Output{satoshis: satoshis, locking_script: script}]
      }

      {:ok, _} = TransactionProcessor.process_tx(tx, [address], [])

      hist = Repo.get_by!(Athanor.Schema.AddressHistory, address: address)
      assert hist.direction == "in"
      assert hist.satoshis == satoshis
    end
  end
end
