defmodule Athanor.Schema.BroadcastTest do
  use Athanor.DataCase, async: true

  alias Athanor.Schema.Broadcast
  import Athanor.Fixtures

  describe "changeset/2" do
    test "valid with required fields" do
      attrs = broadcast_attrs()
      changeset = Broadcast.changeset(%Broadcast{}, attrs)
      assert changeset.valid?
    end

    test "valid with all statuses (incl. Phase 4 relayed/unconfirmed/propagated)" do
      for status <- ~w(pending relayed unconfirmed rejected accepted propagated) do
        attrs = broadcast_attrs(%{status: status})
        changeset = Broadcast.changeset(%Broadcast{}, attrs)
        assert changeset.valid?, "expected #{status} to be valid"
      end
    end

    test "each Phase 4 status persists through changeset/2 to the DB" do
      for status <- ~w(relayed unconfirmed propagated) do
        {:ok, row} =
          %Broadcast{}
          |> Broadcast.changeset(broadcast_attrs(%{status: status}))
          |> Repo.insert()

        assert row.status == status
      end
    end

    test "invalid with bad status" do
      attrs = broadcast_attrs(%{status: "unknown"})
      changeset = Broadcast.changeset(%Broadcast{}, attrs)
      refute changeset.valid?
      assert %{status: _} = errors_on(changeset)
    end

    test "invalid without txid" do
      attrs = broadcast_attrs() |> Map.delete(:txid)
      changeset = Broadcast.changeset(%Broadcast{}, attrs)
      refute changeset.valid?
    end

    test "inserts successfully" do
      broadcast = broadcast_fixture()
      assert broadcast.id != nil
      assert broadcast.status == "pending"
    end
  end

  describe "advance_status/2 (monotonic lattice, never downgrades)" do
    # rank: pending(0) < relayed(1) < unconfirmed(2) < rejected(3) < accepted(4) < propagated(5)

    test "advances to a strictly higher tier" do
      assert Broadcast.advance_status("pending", "relayed") == "relayed"
      assert Broadcast.advance_status("relayed", "accepted") == "accepted"
      assert Broadcast.advance_status("accepted", "propagated") == "propagated"
    end

    test "never downgrades to a lower tier" do
      assert Broadcast.advance_status("accepted", "relayed") == "accepted"
      assert Broadcast.advance_status("propagated", "accepted") == "propagated"
      assert Broadcast.advance_status("relayed", "pending") == "relayed"
    end

    test "reject (3) outranks unconfirmed (2): reject-then-TTL keeps rejected" do
      # An existing rejected row is NOT overwritten by a later TTL :unconfirmed.
      assert Broadcast.advance_status("rejected", "unconfirmed") == "rejected"
    end

    test "TTL-then-reject: an unconfirmed row IS overwritten by a later reject" do
      assert Broadcast.advance_status("unconfirmed", "rejected") == "rejected"
    end

    test "accepted (4) outranks a single peer reject (3)" do
      assert Broadcast.advance_status("accepted", "rejected") == "accepted"
    end
  end
end
