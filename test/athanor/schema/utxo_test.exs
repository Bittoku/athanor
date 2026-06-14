defmodule Athanor.Schema.UtxoTest do
  use Athanor.DataCase, async: true

  alias Athanor.Schema.Utxo
  import Athanor.Fixtures

  describe "changeset/2" do
    test "valid with required fields" do
      attrs = utxo_attrs()
      changeset = Utxo.changeset(%Utxo{}, attrs)
      assert changeset.valid?
    end

    test "valid with token fields" do
      attrs = utxo_attrs(%{token_id: "tok123", token_type: "stas"})
      changeset = Utxo.changeset(%Utxo{}, attrs)
      assert changeset.valid?
    end

    test "invalid without txid" do
      attrs = utxo_attrs() |> Map.delete(:txid)
      changeset = Utxo.changeset(%Utxo{}, attrs)
      refute changeset.valid?
    end

    test "invalid with negative vout" do
      attrs = utxo_attrs(%{vout: -1})
      changeset = Utxo.changeset(%Utxo{}, attrs)
      refute changeset.valid?
      assert %{vout: _} = errors_on(changeset)
    end

    test "invalid with bad token_type" do
      attrs = utxo_attrs(%{token_type: "invalid"})
      changeset = Utxo.changeset(%Utxo{}, attrs)
      refute changeset.valid?
      assert %{token_type: _} = errors_on(changeset)
    end

    test "enforces unique txid+vout" do
      txid = :crypto.strong_rand_bytes(32)
      utxo_fixture(%{txid: txid, vout: 0})

      {:error, changeset} =
        %Utxo{} |> Utxo.changeset(utxo_attrs(%{txid: txid, vout: 0})) |> Repo.insert()

      assert %{txid: ["has already been taken"]} = errors_on(changeset)
    end
  end
end
