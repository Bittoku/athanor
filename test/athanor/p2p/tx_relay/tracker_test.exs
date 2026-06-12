defmodule Athanor.P2P.TxRelay.TrackerTest do
  @moduledoc """
  Tests for the pure relay-lifecycle reducer `Athanor.P2P.TxRelay.Tracker`
  (Phase 4 T4.0, §B). `step(state, event, now_ms) -> {state, actions}` — no
  process, no IO, time injected. Governs one broadcast's life: announce `inv` to
  the chosen targets, serve `getdata` from the pending tx (once per `(txid,
  peer)`), and confirm **propagated** only when `bar ≥ 1` distinct **non-target**
  peers relay the tx back.

  Key invariants under test:
    * a relay-back counts iff its peer is **not** in `announced_to` (target echo
      never counts);
    * `:propagated` fires exactly once, at the `bar`-th distinct non-target;
    * `getdata` is served at most once per `(txid, peer)` (flood dedup);
    * `max_pending` saturates without growing `pending`; `:tick` expires stale
      pending as `:unconfirmed` (but not an already-`propagated` entry).
  """
  use ExUnit.Case, async: true

  alias Athanor.P2P.TxRelay.Tracker

  defp txid(n), do: :binary.copy(<<n>>, 32)
  defp new(opts \\ []), do: Tracker.new(opts)

  # Announce `txid` to `targets` with bar; returns the post-broadcast state.
  defp broadcast(state, txid, targets, bar, now \\ 0) do
    {state, actions} =
      Tracker.step(state, {:broadcast, txid, "raw-#{inspect(txid)}", targets, bar}, now)

    assert actions == Enum.map(targets, &{:send_inv, &1, txid})
    state
  end

  describe "broadcast" do
    test "records pending and announces inv to exactly the targets" do
      {state, actions} =
        Tracker.step(new(), {:broadcast, txid(1), "raw", [:a, :b], 2}, 5)

      assert actions == [{:send_inv, :a, txid(1)}, {:send_inv, :b, txid(1)}]
      assert %{pending: %{} = pending} = state
      entry = pending[txid(1)]
      assert entry.raw == "raw"
      assert entry.announced_to == MapSet.new([:a, :b])
      assert entry.relayed_back == MapSet.new()
      assert entry.served_to == MapSet.new()
      assert entry.bar == 2
      assert entry.propagated? == false
      assert entry.first_at_ms == 5
    end

    test "at max_pending a further broadcast saturates without growing pending" do
      state = broadcast(new(max_pending: 1), txid(1), [:a], 1)
      {state2, actions} = Tracker.step(state, {:broadcast, txid(2), "raw2", [:a], 1}, 1)

      assert actions == [{:saturated, txid(2)}]
      assert Map.keys(state2.pending) == [txid(1)]
    end
  end

  describe "getdata" do
    test "serves the stored raw tx to a new peer, once per (txid, peer)" do
      state = broadcast(new(), txid(1), [:a], 1)

      {state, actions} = Tracker.step(state, {:getdata, txid(1), :p}, 1)
      assert actions == [{:send_tx, :p, state.pending[txid(1)].raw}]

      # A repeat getdata from the same peer is not re-served (flood dedup).
      assert {^state, []} = Tracker.step(state, {:getdata, txid(1), :p}, 2)
    end

    test "ignores getdata for a txid we are not broadcasting" do
      assert {_s, []} = Tracker.step(new(), {:getdata, txid(9), :p}, 0)
    end
  end

  describe "inv / propagation" do
    test "a target echo (peer in announced_to) does not count" do
      state = broadcast(new(), txid(1), [:a, :b], 2)
      assert {_s, []} = Tracker.step(state, {:inv, txid(1), :a}, 1)
    end

    test "N>=3 (bar 2): propagated fires once at the 2nd distinct non-target" do
      state = broadcast(new(), txid(1), [:t], 2)

      # A target echo + one non-target is NOT enough.
      {state, []} = Tracker.step(state, {:inv, txid(1), :t}, 1)
      {state, []} = Tracker.step(state, {:inv, txid(1), :x}, 2)
      # 2nd distinct non-target crosses the bar.
      {state, actions} = Tracker.step(state, {:inv, txid(1), :y}, 3)
      assert actions == [{:propagated, txid(1)}]
      # Once-only: further non-target invs do not re-emit.
      assert {_s, []} = Tracker.step(state, {:inv, txid(1), :z}, 4)
    end

    test "N==2 (bar 1): propagated fires at the 1st non-target" do
      state = broadcast(new(), txid(1), [:t], 1)
      {_s, actions} = Tracker.step(state, {:inv, txid(1), :x}, 1)
      assert actions == [{:propagated, txid(1)}]
    end

    test "bar 0 (single peer): never propagated" do
      state = broadcast(new(), txid(1), [:t], 0)
      assert {_s, []} = Tracker.step(state, {:inv, txid(1), :x}, 1)
    end

    test "a duplicate non-target inv does not double-count toward the bar" do
      state = broadcast(new(), txid(1), [:t], 2)
      {state, []} = Tracker.step(state, {:inv, txid(1), :x}, 1)
      # Same non-target again — still only one distinct relay-back.
      assert {_s, []} = Tracker.step(state, {:inv, txid(1), :x}, 2)
    end
  end

  describe "reject" do
    test "records the rejection for the txid" do
      state = broadcast(new(), txid(1), [:a], 1)
      {_s, actions} = Tracker.step(state, {:reject, txid(1), :a, "16: bad-txns"}, 1)
      assert actions == [{:rejected, txid(1), :a, "16: bad-txns"}]
    end
  end

  describe "tick" do
    test "expires stale pending as :unconfirmed and drops it" do
      state = broadcast(new(ttl_ms: 100), txid(1), [:a], 1, 0)
      {state, actions} = Tracker.step(state, :tick, 101)
      assert actions == [{:unconfirmed, txid(1)}]
      assert state.pending == %{}
    end

    test "keeps a non-expired entry" do
      state = broadcast(new(ttl_ms: 100), txid(1), [:a], 1, 0)
      {state, actions} = Tracker.step(state, :tick, 50)
      assert actions == []
      assert Map.has_key?(state.pending, txid(1))
    end

    test "drops an already-propagated entry on expiry WITHOUT re-emitting unconfirmed" do
      state = broadcast(new(ttl_ms: 100), txid(1), [:t], 1, 0)
      {state, [{:propagated, _}]} = Tracker.step(state, {:inv, txid(1), :x}, 1)
      {state, actions} = Tracker.step(state, :tick, 101)
      assert actions == []
      assert state.pending == %{}
    end
  end
end
