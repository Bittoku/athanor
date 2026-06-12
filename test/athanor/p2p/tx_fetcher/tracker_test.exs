defmodule Athanor.P2P.TxFetcher.TrackerTest do
  @moduledoc """
  Tests for the pure pull-fetch reducer `Athanor.P2P.TxFetcher.Tracker`
  (Phase 5 T5.1, §B). `step(state, event, now_ms) -> {state, actions}` — no
  process, no IO, time injected. Governs one `getdata`-by-txid request: ask up to
  N peers, resolve `{:ok, raw}` on a matching `tx` from an asked peer (the shell
  re-hashes the payload, so a non-matching payload never reaches a pending txid —
  the forgery guard), `:miss` when every asked peer `notfound`s, and `:miss` on
  timeout. Resolve fires exactly once (the entry is dropped on resolve).
  """
  use ExUnit.Case, async: true

  alias Athanor.P2P.TxFetcher.Tracker

  defp txid(n), do: :binary.copy(<<n>>, 32)
  defp new(opts \\ []), do: Tracker.new(opts)

  # Issue a request for `txid` to `peers`; assert the getdata fan-out, return state.
  defp request(state, txid, peers, now \\ 0) do
    {state, actions} = Tracker.step(state, {:request, txid, peers}, now)
    assert actions == Enum.map(peers, &{:send_getdata, &1, txid})
    state
  end

  describe "request" do
    test "records pending and sends getdata to exactly the asked peers" do
      {state, actions} = Tracker.step(new(), {:request, txid(1), [:a, :b]}, 5)

      assert actions == [{:send_getdata, :a, txid(1)}, {:send_getdata, :b, txid(1)}]
      assert %{pending: %{} = pending} = state
      assert pending[txid(1)].asked == MapSet.new([:a, :b])
      assert pending[txid(1)].first_at_ms == 5
    end
  end

  describe "tx" do
    test "a matching tx from an asked peer resolves {:ok, raw} once" do
      state = request(new(), txid(1), [:a, :b])

      {state, actions} = Tracker.step(state, {:tx, txid(1), "raw-bytes", :a}, 1)
      assert actions == [{:resolve, txid(1), {:ok, "raw-bytes"}}]

      # Resolved → entry dropped → a second tx is ignored (resolve-once).
      assert {^state, []} = Tracker.step(state, {:tx, txid(1), "raw-bytes", :b}, 2)
    end

    test "a tx from a peer we did not ask is ignored" do
      state = request(new(), txid(1), [:a])
      assert {_s, []} = Tracker.step(state, {:tx, txid(1), "raw", :stranger}, 1)
    end

    test "a tx for a txid we are not fetching is ignored" do
      assert {_s, []} = Tracker.step(new(), {:tx, txid(9), "raw", :a}, 0)
    end
  end

  describe "notfound" do
    test "resolves :miss only once every asked peer has answered notfound" do
      state = request(new(), txid(1), [:a, :b])

      {state, actions} = Tracker.step(state, {:notfound, txid(1), :a}, 1)
      assert actions == []

      {state, actions} = Tracker.step(state, {:notfound, txid(1), :b}, 2)
      assert actions == [{:resolve, txid(1), :miss}]

      # Dropped on resolve → a late notfound is ignored.
      assert {^state, []} = Tracker.step(state, {:notfound, txid(1), :a}, 3)
    end

    test "a notfound for a non-pending txid is ignored" do
      assert {_s, []} = Tracker.step(new(), {:notfound, txid(9), :a}, 0)
    end
  end

  describe "timeout via :tick" do
    test "an unresolved request older than timeout_ms resolves :miss and is dropped" do
      state = request(new(timeout_ms: 100), txid(1), [:a, :b], 0)

      # Before the deadline: nothing.
      {state, actions} = Tracker.step(state, :tick, 99)
      assert actions == []

      # At/after the deadline: :miss, entry gone.
      {state, actions} = Tracker.step(state, :tick, 100)
      assert actions == [{:resolve, txid(1), :miss}]
      assert state.pending == %{}
    end

    test "a tick does not re-resolve an already-resolved (dropped) request" do
      state = request(new(timeout_ms: 100), txid(1), [:a], 0)
      {state, [{:resolve, _, {:ok, _}}]} = Tracker.step(state, {:tx, txid(1), "raw", :a}, 10)
      assert {^state, []} = Tracker.step(state, :tick, 1_000)
    end
  end
end
