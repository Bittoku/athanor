defmodule Athanor.Schema.WatchingAddressTest do
  use Athanor.DataCase, async: true

  alias Athanor.Schema.WatchingAddress

  describe "changeset/2" do
    test "valid with required address" do
      changeset = WatchingAddress.changeset(%WatchingAddress{}, %{address: "1ABC123"})
      assert changeset.valid?
    end

    test "valid with address and name" do
      changeset =
        WatchingAddress.changeset(%WatchingAddress{}, %{address: "1ABC123", name: "My Wallet"})

      assert changeset.valid?
    end

    test "invalid without address" do
      changeset = WatchingAddress.changeset(%WatchingAddress{}, %{name: "No Address"})
      refute changeset.valid?
      assert %{address: ["can't be blank"]} = errors_on(changeset)
    end

    test "enforces unique address constraint" do
      {:ok, _} = Repo.insert(WatchingAddress.changeset(%WatchingAddress{}, %{address: "1DUP"}))

      {:error, changeset} =
        Repo.insert(WatchingAddress.changeset(%WatchingAddress{}, %{address: "1DUP"}))

      assert %{address: ["has already been taken"]} = errors_on(changeset)
    end
  end
end
