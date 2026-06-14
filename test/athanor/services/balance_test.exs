defmodule Athanor.Services.BalanceTest do
  @moduledoc """
  Covers `Athanor.Services.Balance` — pure DB-backed aggregations.

  Pins the Decimal→integer normalization: `Repo.one(sum(...))` returns a
  `%Decimal{}` when rows exist and `nil` otherwise. Production callers
  (`AddressController`, `WalletChannel`) expect integers — the inconsistent
  return type would have caused subtle JSON-serialization surprises.
  """

  use Athanor.DataCase, async: false

  alias Athanor.Services.Balance
  alias Athanor.Repo
  alias Athanor.Schema.Utxo

  ## ── Helpers ──

  @addr "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"
  @other_addr "1BvBMSEYstWetqTFn5Au4m4GFg7xJaNVN2"

  defp insert_utxo!(attrs) do
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

  describe "get_balance/1 (BSV only)" do
    test "returns 0 (integer) when no UTXOs exist for the address" do
      assert Balance.get_balance(@addr) === 0
    end

    test "sums plain unspent UTXOs and returns an integer" do
      insert_utxo!(%{satoshis: 1000})
      insert_utxo!(%{satoshis: 2500})

      assert Balance.get_balance(@addr) === 3500
    end

    test "excludes spent UTXOs" do
      insert_utxo!(%{satoshis: 1000, is_spent: true})
      insert_utxo!(%{satoshis: 500})

      assert Balance.get_balance(@addr) == 500
    end

    test "excludes any row with a token_type (STAS / STAS3 outputs)" do
      insert_utxo!(%{satoshis: 1000})
      insert_utxo!(%{satoshis: 9_000_000, token_type: "stas3", token_id: "abc"})

      assert Balance.get_balance(@addr) == 1000
    end

    test "excludes forged-issuance rows that have token_type but a nil token_id" do
      insert_utxo!(%{satoshis: 1000})
      insert_utxo!(%{satoshis: 9_999, token_type: "stas3", token_id: nil})

      assert Balance.get_balance(@addr) == 1000
    end

    test "scopes by address" do
      insert_utxo!(%{satoshis: 1000})
      insert_utxo!(%{satoshis: 9_000_000, address: @other_addr})

      assert Balance.get_balance(@addr) == 1000
    end
  end

  describe "get_full_balance/1" do
    test "returns bsv + grouped token rows with integer satoshis" do
      insert_utxo!(%{satoshis: 1000})
      insert_utxo!(%{satoshis: 250, token_type: "stas3", token_id: "tok-a"})
      insert_utxo!(%{satoshis: 750, token_type: "stas3", token_id: "tok-a"})
      insert_utxo!(%{satoshis: 500, token_type: "stas3", token_id: "tok-b"})

      result = Balance.get_full_balance(@addr)

      assert result.bsv === 1000
      assert length(result.tokens) == 2

      a = Enum.find(result.tokens, &(&1.token_id == "tok-a"))
      b = Enum.find(result.tokens, &(&1.token_id == "tok-b"))

      assert a.satoshis === 1000
      assert a.count == 2
      assert b.satoshis === 500
      assert b.count == 1
    end

    test "returns empty tokens list when address holds only BSV" do
      insert_utxo!(%{satoshis: 1000})

      assert %{bsv: 1000, tokens: []} = Balance.get_full_balance(@addr)
    end
  end

  describe "get_balances/1" do
    test "maps each address to its full balance" do
      insert_utxo!(%{satoshis: 1000})
      insert_utxo!(%{satoshis: 250, address: @other_addr})

      result = Balance.get_balances([@addr, @other_addr])

      assert result[@addr].bsv === 1000
      assert result[@other_addr].bsv === 250
    end

    test "returns a zero entry for an address with no UTXOs" do
      result = Balance.get_balances([@addr])
      assert result[@addr] == %{bsv: 0, tokens: []}
    end
  end

  describe "get_token_balances/2" do
    test "groups by token_id and returns integer satoshis" do
      insert_utxo!(%{satoshis: 100, token_type: "stas3", token_id: "tok-a"})
      insert_utxo!(%{satoshis: 200, token_type: "stas3", token_id: "tok-a"})
      insert_utxo!(%{satoshis: 999, token_type: "stas3", token_id: "tok-b"})

      [row] = Balance.get_token_balances(@addr, ["tok-a"])
      assert row.token_id == "tok-a"
      assert row.satoshis === 300
      assert row.count == 2
    end

    test "filters out tokens not in the requested list" do
      insert_utxo!(%{satoshis: 100, token_type: "stas3", token_id: "tok-a"})
      insert_utxo!(%{satoshis: 200, token_type: "stas3", token_id: "tok-b"})

      result = Balance.get_token_balances(@addr, ["tok-a"])
      assert length(result) == 1
      assert Enum.all?(result, &(&1.token_id == "tok-a"))
    end

    test "returns [] when no requested tokens are held" do
      assert Balance.get_token_balances(@addr, ["nothing"]) == []
    end
  end
end
