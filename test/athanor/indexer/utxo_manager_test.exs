defmodule Athanor.Indexer.UtxoManagerTest do
  @moduledoc """
  Covers the stateless UTXO query layer.

  Pins the Decimal→integer normalization in `stats/1.total_satoshis` —
  the production code previously returned `Decimal` for non-empty
  addresses and integer `0` for empty ones, an inconsistency that bled
  into REST/WebSocket responses as a serialization surprise.
  """

  use Athanor.DataCase, async: false
  import Ecto.Query, only: [from: 2]

  alias Athanor.Indexer.UtxoManager
  alias Athanor.Repo
  alias Athanor.Schema.Utxo

  ## ── Helpers ──

  @addr "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"
  @other_addr "1BvBMSEYstWetqTFn5Au4m4GFg7xJaNVN2"

  defp insert_utxo!(attrs \\ %{}) do
    base = %{
      txid: :crypto.strong_rand_bytes(32),
      vout: 0,
      address: @addr,
      satoshis: 1000,
      script_hex: "76a90088ac",
      is_spent: false
    }

    {:ok, u} = %Utxo{} |> Utxo.changeset(Map.merge(base, attrs)) |> Repo.insert()
    u
  end

  ## ── Tests ──

  describe "list_unspent/1" do
    test "returns only unspent UTXOs for the address" do
      a = insert_utxo!(%{satoshis: 100})
      _b = insert_utxo!(%{satoshis: 200, is_spent: true})
      _c = insert_utxo!(%{satoshis: 300, address: @other_addr})

      [only] = UtxoManager.list_unspent(@addr)
      assert only.id == a.id
    end

    test "returns [] when address has no UTXOs" do
      assert UtxoManager.list_unspent(@addr) == []
    end
  end

  describe "list_token_utxos/2" do
    test "returns unspent UTXOs for a token regardless of address when no address given" do
      a = insert_utxo!(%{token_type: "stas3", token_id: "tok", address: @addr})
      b = insert_utxo!(%{token_type: "stas3", token_id: "tok", address: @other_addr})
      _spent = insert_utxo!(%{token_type: "stas3", token_id: "tok", is_spent: true})

      result = UtxoManager.list_token_utxos("tok")
      ids = MapSet.new(result, & &1.id)
      assert ids == MapSet.new([a.id, b.id])
    end

    test "filters by address when one is provided" do
      a = insert_utxo!(%{token_type: "stas3", token_id: "tok", address: @addr})
      _b = insert_utxo!(%{token_type: "stas3", token_id: "tok", address: @other_addr})

      [only] = UtxoManager.list_token_utxos("tok", @addr)
      assert only.id == a.id
    end

    test "excludes other tokens" do
      _other = insert_utxo!(%{token_type: "stas3", token_id: "other"})

      assert UtxoManager.list_token_utxos("tok") == []
    end
  end

  describe "create_utxo/1" do
    test "inserts a new UTXO" do
      attrs = %{
        txid: :crypto.strong_rand_bytes(32),
        vout: 3,
        address: @addr,
        satoshis: 4242,
        script_hex: "76a90088ac"
      }

      assert {:ok, %Utxo{} = u} = UtxoManager.create_utxo(attrs)
      assert u.satoshis == 4242
      assert u.vout == 3
    end

    test "is idempotent on (txid, vout) duplicates (on_conflict: :nothing)" do
      txid = :crypto.strong_rand_bytes(32)
      attrs = %{txid: txid, vout: 0, address: @addr, satoshis: 1000, script_hex: "76a90088ac"}

      assert {:ok, _} = UtxoManager.create_utxo(attrs)
      # Second insert with the same (txid, vout) must not error or duplicate.
      assert {:ok, _} = UtxoManager.create_utxo(attrs)

      count = Repo.aggregate(from(u in Utxo, where: u.txid == ^txid), :count, :id)
      assert count == 1
    end
  end

  describe "spend_utxo/3" do
    test "marks an existing UTXO as spent with the spender txid" do
      u = insert_utxo!()
      spender = :crypto.strong_rand_bytes(32)

      assert {:ok, updated} = UtxoManager.spend_utxo(u.txid, u.vout, spender)
      assert updated.is_spent == true
      assert updated.spent_txid == spender
    end

    test "returns {:error, :not_found} when no UTXO matches" do
      assert {:error, :not_found} = UtxoManager.spend_utxo(:crypto.strong_rand_bytes(32), 0, "x")
    end
  end

  describe "set_stas3_op/3" do
    test "records the spend-type op on the existing UTXO" do
      u = insert_utxo!()

      assert {:ok, updated} = UtxoManager.set_stas3_op(u.txid, u.vout, "transfer")
      assert updated.stas3_op == "transfer"
    end

    test "returns {:error, :not_found} for an unknown UTXO" do
      assert {:error, :not_found} =
               UtxoManager.set_stas3_op(:crypto.strong_rand_bytes(32), 0, "transfer")
    end
  end

  describe "unconfirm_above/1" do
    test "nulls block_height for rows above the threshold and leaves others alone" do
      below = insert_utxo!(%{block_height: 50})
      at = insert_utxo!(%{block_height: 100})
      above_a = insert_utxo!(%{block_height: 101})
      above_b = insert_utxo!(%{block_height: 200})

      assert {affected, nil} = UtxoManager.unconfirm_above(100)
      assert affected == 2

      assert Repo.get!(Utxo, below.id).block_height == 50
      assert Repo.get!(Utxo, at.id).block_height == 100
      assert is_nil(Repo.get!(Utxo, above_a.id).block_height)
      assert is_nil(Repo.get!(Utxo, above_b.id).block_height)
    end
  end

  describe "stats/1" do
    test "returns count=0 and total_satoshis=0 (integer) when address has no UTXOs" do
      assert UtxoManager.stats(@addr) == %{count: 0, total_satoshis: 0}
    end

    test "sums unspent satoshis and counts UTXOs, returning an integer total" do
      insert_utxo!(%{satoshis: 1000})
      insert_utxo!(%{satoshis: 2500})
      insert_utxo!(%{satoshis: 9_999, is_spent: true})

      stats = UtxoManager.stats(@addr)
      assert stats.count == 2
      assert stats.total_satoshis === 3500
    end
  end
end
