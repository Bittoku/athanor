defmodule Athanor.Indexer.TipController.MachineTest do
  @moduledoc """
  Tests for `Athanor.Indexer.TipController.Machine` (Phase 7 F7.2 T7.0) — the pure
  authority state machine. `step(state, event) -> {state, [action]}`:

    * phases — `:bootstrapping | :syncing | :synced`;
    * events — `:tick | {:hint, source} | {:cycle_result, result}` where `result` is
      `:bootstrapped | :synced | :progressed | :deferred`;
    * actions — `:reconcile | :noop`.

  Coalescing: a tick/hint while a cycle is in flight does not start a second cycle;
  it marks `pending` so exactly one follow-up runs when the cycle returns. A
  `:progressed` result schedules an immediate follow-up (keep catching up); a
  `:synced` result settles; a `:deferred` result waits for the next tick.
  """
  use ExUnit.Case, async: true

  alias Athanor.Indexer.TipController.Machine

  defp new, do: Machine.new()

  describe "triggering a cycle" do
    test "a tick on an idle machine schedules a reconcile and marks a cycle in flight" do
      {m, actions} = Machine.step(new(), :tick)
      assert actions == [:reconcile]
      assert m.in_flight
    end

    test "a hint on an idle machine schedules a reconcile" do
      {m, actions} = Machine.step(new(), {:hint, :p2p})
      assert actions == [:reconcile]
      assert m.in_flight
    end

    test "a tick/hint while a cycle is in flight is coalesced (no second reconcile), marking pending" do
      {m, [:reconcile]} = Machine.step(new(), :tick)
      {m, actions} = Machine.step(m, {:hint, :zmq})
      assert actions == [:noop]
      assert m.in_flight
      assert m.pending
    end
  end

  describe "folding cycle results" do
    test "a :synced result settles to :synced and ends the in-flight cycle" do
      {m, [:reconcile]} = Machine.step(new(), :tick)
      {m, actions} = Machine.step(m, {:cycle_result, :synced})
      assert actions == [:noop]
      assert m.phase == :synced
      refute m.in_flight
    end

    test "a :progressed result keeps :syncing and schedules an immediate follow-up reconcile" do
      {m, [:reconcile]} = Machine.step(new(), :tick)
      {m, actions} = Machine.step(m, {:cycle_result, :progressed})
      assert actions == [:reconcile]
      assert m.phase == :syncing
      assert m.in_flight
    end

    test "a :deferred result preserves the phase and waits (no follow-up) for the next tick" do
      # Establish :syncing first (a :deferred preserves the phase, it does not promote
      # bootstrapping → syncing — only :bootstrapped does that).
      {m, [:reconcile]} = Machine.step(new(), :tick)
      {m, _} = Machine.step(m, {:cycle_result, :bootstrapped})
      assert m.phase == :syncing

      {m, [:reconcile]} = Machine.step(m, :tick)
      {m, actions} = Machine.step(m, {:cycle_result, :deferred})
      assert actions == [:noop]
      assert m.phase == :syncing
      refute m.in_flight
    end

    test "a :bootstrapped result moves from :bootstrapping to :syncing" do
      m = new()
      assert m.phase == :bootstrapping
      {m, [:reconcile]} = Machine.step(m, :tick)
      {m, _} = Machine.step(m, {:cycle_result, :bootstrapped})
      assert m.phase == :syncing
    end

    test "a pending tick/hint during a cycle triggers exactly one follow-up on completion" do
      {m, [:reconcile]} = Machine.step(new(), :tick)
      {m, [:noop]} = Machine.step(m, {:hint, :p2p})
      assert m.pending
      # Cycle returns :synced, but a hint was pending → one follow-up reconcile.
      {m, actions} = Machine.step(m, {:cycle_result, :synced})
      assert actions == [:reconcile]
      assert m.in_flight
      refute m.pending
    end
  end

  describe "synced ⇄ syncing" do
    test "a hint while :synced re-checks; a :progressed result drops back to :syncing" do
      {m, [:reconcile]} = Machine.step(new(), :tick)
      {m, _} = Machine.step(m, {:cycle_result, :synced})
      assert m.phase == :synced

      {m, [:reconcile]} = Machine.step(m, {:hint, :p2p})
      {m, [:reconcile]} = Machine.step(m, {:cycle_result, :progressed})
      assert m.phase == :syncing
    end
  end
end
