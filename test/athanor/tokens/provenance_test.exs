defmodule Athanor.Tokens.ProvenanceTest do
  @moduledoc """
  Covers `Provenance.verify/2`, `lineage/2`, and `token_stats/1`.

  The first two delegate to `B2gResolver`, so we seed the local DB with
  walkable chains and let the resolver hit them via its default fetcher.
  `token_stats/1` is a pure aggregation over `utxos`.
  """

  use Athanor.DataCase, async: false

  alias Athanor.Tokens.Provenance
  alias Athanor.Repo
  alias Athanor.Schema.{Utxo, MetaTransaction}
  alias BSV.Tokens.Script.Stas3Builder

  ## ── Helpers ──

  defp stas3_lock(owner, redemption) do
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

    script
  end

  defp p2pkh_lock(pkh), do: BSV.Script.p2pkh_lock(pkh)

  defp tx_with(inputs, outputs) do
    %BSV.Transaction{version: 1, lock_time: 0, inputs: inputs, outputs: outputs}
  end

  defp input(source_txid, source_vout \\ 0) do
    %BSV.Transaction.Input{
      source_txid: source_txid,
      source_tx_out_index: source_vout,
      sequence_number: 0xFFFFFFFF,
      unlocking_script: %BSV.Script{chunks: []}
    }
  end

  defp output(satoshis, script) do
    %BSV.Transaction.Output{satoshis: satoshis, locking_script: script}
  end

  defp persist!(tx) do
    {:ok, _} =
      %MetaTransaction{}
      |> MetaTransaction.changeset(%{
        txid: BSV.Transaction.txid_binary(tx),
        hex: BSV.Transaction.to_hex(tx),
        timestamp: System.os_time(:second),
        is_confirmed: true
      })
      |> Repo.insert()

    tx
  end

  defp insert_utxo!(token_id, attrs) do
    base = %{
      txid: :crypto.strong_rand_bytes(32),
      vout: 0,
      address: "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa",
      satoshis: 1000,
      script_hex: "76a91462e907b15cbf27d5425399ebf6f0fb50ebb88f1888ac",
      token_id: token_id,
      token_type: "stas3",
      is_spent: false
    }

    {:ok, u} =
      %Utxo{}
      |> Utxo.changeset(Map.merge(base, attrs))
      |> Repo.insert()

    u
  end

  ## ── Tests ──

  describe "verify/2" do
    test "returns a depth-1 result for an immediate non-STAS issuance" do
      proto = :binary.copy(<<0xAB>>, 20)

      tx =
        tx_with([input(:binary.copy(<<0x11>>, 32))], [output(50_000, p2pkh_lock(proto))])
        |> persist!()

      assert {:ok, %{valid: true, depth: 1, chain: chain}} =
               Provenance.verify(BSV.Transaction.txid_binary(tx), 0)

      assert length(chain) == 1
    end

    test "returns the resolved chain depth for a multi-hop STAS3 lineage" do
      proto = :binary.copy(<<0xCD>>, 20)

      genesis =
        tx_with([input(:binary.copy(<<0x11>>, 32))], [output(50_000, p2pkh_lock(proto))])
        |> persist!()

      tip =
        tx_with(
          [input(BSV.Transaction.txid_binary(genesis), 0)],
          [output(1000, stas3_lock(:binary.copy(<<0x33>>, 20), proto))]
        )
        |> persist!()

      assert {:ok, %{valid: true, depth: 2}} =
               Provenance.verify(BSV.Transaction.txid_binary(tip), 0)
    end

    test "returns {:error, _} when the output index doesn't exist" do
      tx =
        tx_with([input(:binary.copy(<<0x11>>, 32))], [
          output(50_000, p2pkh_lock(:binary.copy(<<0xA>>, 20)))
        ])
        |> persist!()

      assert {:error, :output_not_found} = Provenance.verify(BSV.Transaction.txid_binary(tx), 5)
    end
  end

  describe "lineage/2" do
    test "returns the resolved chain when provenance is intact" do
      proto = :binary.copy(<<0xEF>>, 20)

      tx =
        tx_with([input(:binary.copy(<<0x11>>, 32))], [output(50_000, p2pkh_lock(proto))])
        |> persist!()

      assert {:ok, chain} = Provenance.lineage(BSV.Transaction.txid_binary(tx), 0)
      assert is_list(chain)
      assert length(chain) == 1
    end

    test "propagates the resolver's :error tuple" do
      tx =
        tx_with([input(:binary.copy(<<0x11>>, 32))], [
          output(50_000, p2pkh_lock(:binary.copy(<<0xA>>, 20)))
        ])
        |> persist!()

      assert {:error, :output_not_found} = Provenance.lineage(BSV.Transaction.txid_binary(tx), 99)
    end
  end

  describe "token_stats/1" do
    test "returns zeros when no UTXOs exist for the token" do
      stats = Provenance.token_stats("nonexistent-token")
      assert stats.token_id == "nonexistent-token"
      assert stats.unspent_count == 0
      assert stats.total_satoshis == 0
      assert stats.spent_count == 0
    end

    test "counts unspent and spent UTXOs separately" do
      tok = "tally-token"
      insert_utxo!(tok, %{is_spent: false, satoshis: 1000})
      insert_utxo!(tok, %{is_spent: false, satoshis: 2500})
      insert_utxo!(tok, %{is_spent: true, satoshis: 500})

      stats = Provenance.token_stats(tok)
      assert stats.unspent_count == 2
      assert stats.spent_count == 1
    end

    test "sums satoshis only across unspent rows" do
      tok = "sum-token"
      insert_utxo!(tok, %{is_spent: false, satoshis: 1000})
      insert_utxo!(tok, %{is_spent: false, satoshis: 2500})
      insert_utxo!(tok, %{is_spent: true, satoshis: 99_999})

      stats = Provenance.token_stats(tok)
      assert stats.total_satoshis == 3500
    end

    test "ignores UTXOs belonging to other tokens" do
      tok = "tracked-token"
      insert_utxo!(tok, %{is_spent: false, satoshis: 1000})
      insert_utxo!("other-token", %{is_spent: false, satoshis: 1_000_000})

      stats = Provenance.token_stats(tok)
      assert stats.unspent_count == 1
      assert stats.total_satoshis == 1000
    end
  end
end
