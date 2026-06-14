defmodule Athanor.Indexer.TransactionFilterTest do
  @moduledoc """
  Covers `TransactionFilter.matches?/1` — the synchronous hot path that
  every incoming transaction goes through. Tests populate ETS directly
  (same approach as `transaction_filter_p2mpkh_test.exs`) so they do
  not depend on the GenServer being started.

  The async `process_raw_tx/1` cast and the `add_address` / `add_token`
  serializer calls are covered indirectly via the existing
  TransactionProcessor tests, which start the real GenServer.
  """

  use ExUnit.Case, async: false

  alias Athanor.Indexer.TransactionFilter
  alias BSV.Tokens.Script.{Stas3Builder, Templates}

  setup do
    for table <- [:watched_addresses, :watched_tokens] do
      case :ets.whereis(table) do
        :undefined -> :ets.new(table, [:set, :public, :named_table])
        _ -> :ets.delete_all_objects(table)
      end
    end

    :ok
  end

  ## ── Helpers ──

  defp p2pkh_output(pkh, satoshis) do
    %BSV.Transaction.Output{satoshis: satoshis, locking_script: BSV.Script.p2pkh_lock(pkh)}
  end

  defp p2mpkh_output(mpkh, satoshis) do
    {:ok, script} = BSV.Script.from_binary(Templates.p2mpkh_locking_script(mpkh))
    %BSV.Transaction.Output{satoshis: satoshis, locking_script: script}
  end

  defp stas3_output(owner, redemption, satoshis) do
    {:ok, script} =
      Stas3Builder.build_stas3_locking_script(
        owner,
        redemption,
        nil,
        false,
        %BSV.Tokens.ScriptFlags{},
        [],
        []
      )

    %BSV.Transaction.Output{satoshis: satoshis, locking_script: script}
  end

  defp tx(outputs) do
    %BSV.Transaction{
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
      outputs: outputs
    }
  end

  ## ── Tests ──

  describe "matches?/1 — address matching" do
    test "returns empty when no outputs match any watched address" do
      tx = tx([p2pkh_output(:binary.copy(<<0xAA>>, 20), 1000)])

      assert {[], []} = TransactionFilter.matches?(tx)
    end

    test "matches a watched address paid via a P2PKH output" do
      pkh = :binary.copy(<<0x77>>, 20)
      address = BSV.Base58.check_encode(pkh, 0x00)
      :ets.insert(:watched_addresses, {address, true})

      tx = tx([p2pkh_output(pkh, 1000)])

      assert {[^address], []} = TransactionFilter.matches?(tx)
    end

    test "deduplicates when the same watched address appears in multiple outputs" do
      pkh = :binary.copy(<<0x88>>, 20)
      address = BSV.Base58.check_encode(pkh, 0x00)
      :ets.insert(:watched_addresses, {address, true})

      tx = tx([p2pkh_output(pkh, 100), p2pkh_output(pkh, 200), p2pkh_output(pkh, 300)])

      {addrs, _} = TransactionFilter.matches?(tx)
      assert addrs == [address]
    end

    test "ignores unwatched addresses even when other outputs do match" do
      watched_pkh = :binary.copy(<<0x11>>, 20)
      unwatched_pkh = :binary.copy(<<0x22>>, 20)
      watched_address = BSV.Base58.check_encode(watched_pkh, 0x00)
      :ets.insert(:watched_addresses, {watched_address, true})

      tx = tx([p2pkh_output(unwatched_pkh, 100), p2pkh_output(watched_pkh, 200)])

      {addrs, _} = TransactionFilter.matches?(tx)
      assert addrs == [watched_address]
    end
  end

  describe "matches?/1 — token matching" do
    test "matches a watched STAS3 token by protoID hex" do
      proto = :binary.copy(<<0xCD>>, 20)
      proto_hex = Base.encode16(proto, case: :lower)
      :ets.insert(:watched_tokens, {proto_hex, true})

      owner = :binary.copy(<<0xEE>>, 20)
      tx = tx([stas3_output(owner, proto, 1000)])

      assert {[], [^proto_hex]} = TransactionFilter.matches?(tx)
    end

    test "deduplicates the same STAS3 token across multiple outputs" do
      proto = :binary.copy(<<0xCC>>, 20)
      proto_hex = Base.encode16(proto, case: :lower)
      :ets.insert(:watched_tokens, {proto_hex, true})

      tx =
        tx([
          stas3_output(:binary.copy(<<0xA0>>, 20), proto, 100),
          stas3_output(:binary.copy(<<0xA1>>, 20), proto, 200)
        ])

      {_, tokens} = TransactionFilter.matches?(tx)
      assert tokens == [proto_hex]
    end

    test "ignores an unwatched STAS3 protoID" do
      proto = :binary.copy(<<0xDD>>, 20)
      # NOT added to :watched_tokens
      tx = tx([stas3_output(:binary.copy(<<0xEE>>, 20), proto, 1000)])

      assert {[], []} = TransactionFilter.matches?(tx)
    end
  end

  describe "matches?/1 — combined" do
    test "returns BOTH a matched address and a matched token from one tx" do
      pkh = :binary.copy(<<0xAB>>, 20)
      address = BSV.Base58.check_encode(pkh, 0x00)
      proto = :binary.copy(<<0xCD>>, 20)
      proto_hex = Base.encode16(proto, case: :lower)
      :ets.insert(:watched_addresses, {address, true})
      :ets.insert(:watched_tokens, {proto_hex, true})

      tx =
        tx([
          p2pkh_output(pkh, 1000),
          stas3_output(:binary.copy(<<0xEE>>, 20), proto, 500)
        ])

      assert {[^address], [^proto_hex]} = TransactionFilter.matches?(tx)
    end

    test "matches a watched address via a P2MPKH output (STAS 3.0 v0.1 §10.2)" do
      mpkh = :binary.copy(<<0x7C>>, 20)
      address = BSV.Base58.check_encode(mpkh, 0x00)
      :ets.insert(:watched_addresses, {address, true})

      tx = tx([p2mpkh_output(mpkh, 4200)])

      {addrs, _} = TransactionFilter.matches?(tx)
      assert address in addrs
    end
  end

  describe "matches?/1 — malformed script tolerance" do
    test "returns empty (does not crash) on an output with a script we cannot parse" do
      junk = %BSV.Transaction.Output{
        satoshis: 1,
        locking_script: %BSV.Script{chunks: [<<0xFF>>]}
      }

      assert {[], []} = TransactionFilter.matches?(tx([junk]))
    end

    test "a malformed output does NOT lose matches from sibling outputs in the same tx" do
      # Regression for an earlier crash where BSV.Script.to_binary/1 raised
      # at the top of `scan_outputs`, aborting the reduce before any sibling
      # output could be checked. Result was: a single malformed output (e.g.
      # a non-standard data carrier) silently lost ALL legitimate matches
      # from the same transaction.
      pkh = :binary.copy(<<0x99>>, 20)
      address = BSV.Base58.check_encode(pkh, 0x00)
      :ets.insert(:watched_addresses, {address, true})

      junk = %BSV.Transaction.Output{
        satoshis: 1,
        locking_script: %BSV.Script{chunks: [<<0xFF>>]}
      }

      tx = tx([junk, p2pkh_output(pkh, 1000)])

      {addrs, _} = TransactionFilter.matches?(tx)
      assert addrs == [address]
    end
  end

  describe "watching_address? / watching_token?" do
    test "watching_address? reads ETS membership" do
      :ets.insert(:watched_addresses, {"1Foo", true})

      assert TransactionFilter.watching_address?("1Foo") == true
      assert TransactionFilter.watching_address?("1Bar") == false
    end

    test "watching_token? reads ETS membership" do
      :ets.insert(:watched_tokens, {"deadbeef", true})

      assert TransactionFilter.watching_token?("deadbeef") == true
      assert TransactionFilter.watching_token?("cafef00d") == false
    end
  end
end
