defmodule Athanor.P2P.MempoolObserver.TrackerTest do
  @moduledoc """
  Tests for the pure mempool request-lifecycle reducer
  `Athanor.P2P.MempoolObserver.Tracker` (Phase 3 T3.2, §B). `step(state, event,
  now_ms) -> {state, actions}` with no process and no IO; time is injected.

  Peer-matched delivery (the !11 review's blocker 3): an outstanding request is
  bound to the peer it was sent to; a `tx`/`notfound` from a different peer is
  ignored and leaves the request outstanding.

  Key invariant (the !10 review's blocker 2): `seen` is set **only** on a
  successful `tx`. A failure (`notfound`/timeout/peer-down) clears `outstanding`
  without marking `seen`, so a *different* peer's later `inv` can re-request —
  while a flood of *simultaneous* `inv`s is still collapsed to one request.
  """
  use ExUnit.Case, async: true

  alias Athanor.P2P.MempoolObserver.Tracker

  defp txid(n), do: :binary.copy(<<n>>, 32)
  defp new(opts \\ []), do: Tracker.new(opts)

  describe "inv" do
    test "a fresh inv requests via getdata and marks it outstanding" do
      {state, actions} = Tracker.step(new(), {:inv, txid(1), :peerA}, 0)

      assert actions == [{:getdata, :peerA, txid(1)}]
      assert {state, []} == Tracker.step(state, {:inv, txid(1), :peerB}, 1)
    end

    test "an inv for an already-seen (completed) txid is ignored" do
      {state, _} = Tracker.step(new(), {:inv, txid(1), :peerA}, 0)
      {state, [{:ingest, _}]} = Tracker.step(state, {:tx, txid(1), "raw", :peerA}, 0)

      assert {^state, []} = Tracker.step(state, {:inv, txid(1), :peerB}, 1)
    end

    test "beyond the token budget, excess invs are dropped (no getdata)" do
      {state, [{:getdata, _, _}]} = Tracker.step(new(max_tokens: 1), {:inv, txid(1), :peerA}, 0)
      assert {^state, []} = Tracker.step(state, {:inv, txid(2), :peerA}, 0)
    end
  end

  describe "tx" do
    test "a tx for an outstanding txid ingests, clears it, and marks it seen" do
      {state, _} = Tracker.step(new(), {:inv, txid(1), :peerA}, 0)
      {state, actions} = Tracker.step(state, {:tx, txid(1), "raw-bytes", :peerA}, 5)

      assert actions == [{:ingest, "raw-bytes"}]
      # Cleared from outstanding; re-inv now ignored (seen).
      assert {^state, []} = Tracker.step(state, {:inv, txid(1), :peerB}, 6)
    end

    test "an unsolicited / duplicate tx is ignored" do
      assert {_state, []} = Tracker.step(new(), {:tx, txid(9), "raw", :peerA}, 0)
    end
  end

  describe "failure paths clear outstanding without marking seen" do
    test "notfound clears the request" do
      {state, _} = Tracker.step(new(), {:inv, txid(1), :peerA}, 0)
      {state, actions} = Tracker.step(state, {:notfound, txid(1), :peerA}, 1)
      assert actions == []
      # Not seen → a later tx would NOT ingest (no outstanding) but a new inv can re-request.
      assert {_s, [{:getdata, :peerB, _}]} = Tracker.step(state, {:inv, txid(1), :peerB}, 2)
    end

    test "a per-request timeout (matching the outstanding generation) clears the request" do
      # The inv at now=0 stores outstanding {peerA, 0}; the timeout carries that
      # same identity (peer + requested_at).
      {state, _} = Tracker.step(new(), {:inv, txid(1), :peerA}, 0)
      {state, []} = Tracker.step(state, {:timeout, txid(1), :peerA, 0}, 100)
      assert {_s, [{:getdata, :peerB, _}]} = Tracker.step(state, {:inv, txid(1), :peerB}, 101)
    end

    test "a stale timeout (wrong generation) does NOT erase a newer request (blocker 2)" do
      # peerA advertises (outstanding {peerA, 10}); notfound clears it; peerB
      # re-advertises (outstanding {peerB, 12}). peerA's stale timer fires with
      # the OLD identity {peerA, 10} — it must not touch peerB's request.
      {state, _} = Tracker.step(new(), {:inv, txid(1), :peerA}, 10)
      {state, []} = Tracker.step(state, {:notfound, txid(1), :peerA}, 11)
      {state, [{:getdata, :peerB, _}]} = Tracker.step(state, {:inv, txid(1), :peerB}, 12)

      {state, []} = Tracker.step(state, {:timeout, txid(1), :peerA, 10}, 13)

      # peerB's request survived → its tx still ingests.
      {_state, ingest} = Tracker.step(state, {:tx, txid(1), "raw-B", :peerB}, 14)
      assert ingest == [{:ingest, "raw-B"}]
    end

    test "peer-down clears only that peer's outstanding requests" do
      {state, _} = Tracker.step(new(), {:inv, txid(1), :peerA}, 0)
      {state, _} = Tracker.step(state, {:inv, txid(2), :peerB}, 0)
      {state, []} = Tracker.step(state, {:peer_down, :peerA}, 1)

      # peerA's txid(1) is re-requestable; peerB's txid(2) is still outstanding.
      assert {_s, [{:getdata, :peerC, _}]} = Tracker.step(state, {:inv, txid(1), :peerC}, 2)
      assert {^state, []} = Tracker.step(state, {:inv, txid(2), :peerC}, 2)
    end
  end

  describe "peer-matched lifecycle (the !11 review's blocker 3)" do
    test "a notfound from the wrong peer does NOT cancel the asked peer's request" do
      # Asked peerA for txid(1). A stranger peerB says notfound.
      {state, _} = Tracker.step(new(), {:inv, txid(1), :peerA}, 0)
      {state, actions} = Tracker.step(state, {:notfound, txid(1), :peerB}, 1)

      # The wrong-peer notfound is ignored and the request stays outstanding,
      # so peerA's later tx still ingests.
      assert actions == []
      {_state, ingest} = Tracker.step(state, {:tx, txid(1), "raw", :peerA}, 2)
      assert ingest == [{:ingest, "raw"}]
    end

    test "a tx from the wrong peer does NOT satisfy the asked peer's request" do
      # Asked peerA for txid(1). An unsolicited tx arrives from peerB.
      {state, _} = Tracker.step(new(), {:inv, txid(1), :peerA}, 0)
      {state, actions} = Tracker.step(state, {:tx, txid(1), "raw-from-B", :peerB}, 1)

      # Ignored; request stays outstanding so the asked peer can still deliver.
      assert actions == []
      {_state, ingest} = Tracker.step(state, {:tx, txid(1), "raw-from-A", :peerA}, 2)
      assert ingest == [{:ingest, "raw-from-A"}]
    end

    test "the correct peer's notfound still clears the request (recovery preserved)" do
      {state, _} = Tracker.step(new(), {:inv, txid(1), :peerA}, 0)
      {state, []} = Tracker.step(state, {:notfound, txid(1), :peerA}, 1)
      assert {_s, [{:getdata, :peerB, _}]} = Tracker.step(state, {:inv, txid(1), :peerB}, 2)
    end
  end

  describe "recovery (blocker 2)" do
    test "notfound -> another peer's inv -> getdata" do
      {s, _} = Tracker.step(new(), {:inv, txid(1), :peerA}, 0)
      {s, []} = Tracker.step(s, {:notfound, txid(1), :peerA}, 1)
      {_s, actions} = Tracker.step(s, {:inv, txid(1), :peerB}, 2)
      assert actions == [{:getdata, :peerB, txid(1)}]
    end

    test "peer_down -> another peer's inv -> getdata" do
      {s, _} = Tracker.step(new(), {:inv, txid(1), :peerA}, 0)
      {s, []} = Tracker.step(s, {:peer_down, :peerA}, 1)
      {_s, actions} = Tracker.step(s, {:inv, txid(1), :peerB}, 2)
      assert actions == [{:getdata, :peerB, txid(1)}]
    end
  end

  describe "tick" do
    test "refills the token bucket and expires the seen set" do
      # Exhaust the single token, drop the next inv.
      {s, _} = Tracker.step(new(max_tokens: 1, seen_ttl_ms: 10), {:inv, txid(1), :peerA}, 0)
      {s, []} = Tracker.step(s, {:inv, txid(2), :peerA}, 0)

      # Tick refills → the dropped txid can now be requested.
      {s, []} = Tracker.step(s, :tick, 1)
      assert {s, [{:getdata, :peerA, _}]} = Tracker.step(s, {:inv, txid(2), :peerA}, 1)

      # Complete it, then let its `seen` TTL expire via a later tick → re-inv works.
      {s, [{:ingest, _}]} = Tracker.step(s, {:tx, txid(2), "raw", :peerA}, 1)
      assert {^s, []} = Tracker.step(s, {:inv, txid(2), :peerA}, 2)
      {s, []} = Tracker.step(s, :tick, 100)
      assert {_s, [{:getdata, :peerA, _}]} = Tracker.step(s, {:inv, txid(2), :peerA}, 100)
    end
  end
end
