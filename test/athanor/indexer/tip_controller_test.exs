defmodule Athanor.Indexer.TipControllerTest do
  @moduledoc """
  Phase 7 F7.2 (T7.3) — the `TipController` GenServer that wires the pure
  `Machine` + `Reconcile` + `BlockProcessor.apply_branch/2` and debounced hints.

  All collaborators are injected (no real RPC/DB): `:rpc_height`, `:rpc_hash_at`,
  `:local_height`, `:local_hash_at`, `:apply_fun` (captures the `apply_branch`
  call). Proves: a hint triggers a reconcile and **only the RPC-confirmed branch**
  is applied; a deferred catch-up is **replayed to the node tip** without a new
  hint; RPC failure / a failed apply fail closed (`:deferred`, no crash).

  `async: false` (singleton-style GenServer per test via `start_supervised!`); a
  bounded `drain/1` over `:sys.get_state` synchronises the self-scheduled cycles
  (no `Process.sleep`).
  """
  use ExUnit.Case, async: false

  alias Athanor.Indexer.TipController

  defp hex(height), do: Base.encode16(<<height::16>>, case: :lower)

  defp start_controller(opts) do
    test = self()

    defaults = [
      rpc_height: fn -> {:ok, 5} end,
      rpc_hash_at: fn _ -> nil end,
      local_height: fn -> 0 end,
      local_hash_at: fn _ -> nil end,
      apply_fun: fn _proc, arg ->
        send(test, {:applied, arg})
        {:ok, length(arg.connect)}
      end,
      # These reconcile-only unit tests run without a DB sandbox, so start already
      # bootstrapped (the atomic capture path is covered by tip_controller_bootstrap_test).
      bootstrapped: true,
      processor: :ignored_processor,
      batch: 10,
      tick_interval_ms: 60_000
    ]

    start_supervised!({TipController, Keyword.merge(defaults, opts)},
      id: {TipController, make_ref()}
    )
  end

  # Drain the self-scheduled :run_cycle messages until the machine settles
  # (idle: not in flight) or a bound is hit.
  defp drain(pid, n \\ 25)
  defp drain(pid, 0), do: :sys.get_state(pid)

  defp drain(pid, n) do
    state = :sys.get_state(pid)
    if state.machine.in_flight or state.machine.pending, do: drain(pid, n - 1), else: state
  end

  test "a hint triggers a reconcile and applies the RPC-confirmed catch-up branch" do
    node_hashes = %{5 => hex(5), 6 => hex(6), 7 => hex(7)}

    pid =
      start_controller(
        rpc_height: fn -> {:ok, 7} end,
        rpc_hash_at: fn h -> node_hashes[h] end,
        local_height: fn -> 5 end,
        # local agrees with the node at the tip (height 5) → pure catch-up.
        local_hash_at: fn h -> if(h == 5, do: hex(5), else: nil) end
      )

    GenServer.cast(pid, {:hint, :p2p})
    _ = drain(pid)

    # Only the node-confirmed branch (heights 6,7) is applied — no rollback.
    assert_received {:applied, %{rollback_to: nil, connect: connect}}
    assert connect == [Base.decode16!(hex(6)), Base.decode16!(hex(7))]
  end

  test "a deferred catch-up is replayed to the node tip without a new hint" do
    # Start far enough below the tip that catch-up needs >1 batch: each
    # `{:ok, last_height < node_tip}` folds to :progressed and self-schedules a
    # replay until `{:ok, node_tip}` folds to :synced.
    {:ok, agent} = Agent.start_link(fn -> 80 end)
    node_hashes = for h <- 0..100, into: %{}, do: {h, hex(h)}

    pid =
      start_controller(
        rpc_height: fn -> {:ok, 100} end,
        rpc_hash_at: fn h -> node_hashes[h] end,
        local_height: fn -> Agent.get(agent, & &1) end,
        # local matches the node up to its current height (no divergence).
        local_hash_at: fn h ->
          if h <= Agent.get(agent, & &1), do: node_hashes[h], else: nil
        end,
        apply_fun: fn _proc, arg ->
          Agent.update(agent, &(&1 + length(arg.connect)))
          {:ok, Agent.get(agent, & &1)}
        end,
        batch: 10
      )

    # One hint; the controller catches the index all the way up to the node tip
    # via self-scheduled follow-ups (10 blocks/cycle), no further announcement.
    GenServer.cast(pid, {:hint, :p2p})
    state = drain(pid)

    assert Agent.get(agent, & &1) == 100
    assert state.machine.phase == :synced
  end

  test "RPC failure fails closed: deferred, no apply, no crash" do
    pid =
      start_controller(
        rpc_height: fn -> {:error, :node_down} end,
        apply_fun: fn _p, _a -> flunk("apply_branch must not run when RPC is unavailable") end
      )

    ref = Process.monitor(pid)
    GenServer.cast(pid, {:hint, :zmq})
    state = drain(pid)

    refute_received {:DOWN, ^ref, :process, ^pid, _}
    refute state.machine.in_flight
  end

  test "a failed apply folds to :deferred and does not advance/crash" do
    node_hashes = %{5 => hex(5), 6 => hex(6)}

    pid =
      start_controller(
        rpc_height: fn -> {:ok, 6} end,
        rpc_hash_at: fn h -> node_hashes[h] end,
        local_height: fn -> 5 end,
        local_hash_at: fn h -> if(h == 5, do: hex(5), else: nil) end,
        apply_fun: fn _p, _a -> {:error, :rpc_block_unavailable} end
      )

    GenServer.cast(pid, {:hint, :p2p})
    state = drain(pid)

    # The apply failed → the cycle deferred (no advancement) and did not schedule a
    # follow-up; the controller is idle and uncrashed (phase unchanged — the
    # :bootstrapping → :syncing transition is driven by progress, wired in T7.S).
    refute state.machine.in_flight
    assert state.machine.phase == :bootstrapping
  end

  test "a :synced cycle settles the machine to :synced" do
    pid =
      start_controller(
        rpc_height: fn -> {:ok, 5} end,
        rpc_hash_at: fn h -> %{5 => hex(5)}[h] end,
        local_height: fn -> 5 end,
        local_hash_at: fn h -> %{5 => hex(5)}[h] end,
        apply_fun: fn _p, _a -> flunk("nothing to apply when already synced") end
      )

    GenServer.cast(pid, {:hint, :p2p})
    state = drain(pid)
    assert state.machine.phase == :synced
  end

  test "notify_tip/1 (the HeadersChain :on_tip sink) hints the controller as :p2p" do
    node_hashes = %{5 => hex(5), 6 => hex(6)}

    pid =
      start_controller(
        name: :tc_notify_test,
        rpc_height: fn -> {:ok, 6} end,
        rpc_hash_at: fn h -> node_hashes[h] end,
        local_height: fn -> 5 end,
        local_hash_at: fn h -> if(h == 5, do: hex(5), else: nil) end
      )

    # notify_tip is the arity-1 on_tip wiring; it must reach this controller.
    TipController.notify_tip(:tc_notify_test, {:extend, [<<0::256>>]})
    _ = drain(pid)
    assert_received {:applied, %{connect: [_ | _]}}
  end

  # ── note-1049 B2: fold the explicit `apply_branch/2` result contract ──
  # The controller's authority phase must be driven by the ordered mutation owner's
  # synchronous result, not inferred from a `local_height` side-effect read.

  test "{:ok, last_height} at the node tip folds to :synced with no self-scheduled replay" do
    node_hashes = %{5 => hex(5), 6 => hex(6), 7 => hex(7)}
    {:ok, local} = Agent.start_link(fn -> 5 end)
    {:ok, calls} = Agent.start_link(fn -> 0 end)

    # The startup `:ensure_bootstrap` continue is itself the single reconcile trigger
    # here (no manual hint), so the apply count proves the *fold*: a result at the
    # node tip settles to :synced and does NOT self-schedule another cycle.
    pid =
      start_controller(
        rpc_height: fn -> {:ok, 7} end,
        rpc_hash_at: fn h -> node_hashes[h] end,
        local_height: fn -> Agent.get(local, & &1) end,
        local_hash_at: fn h -> if h <= Agent.get(local, & &1), do: node_hashes[h], else: nil end,
        # The apply reaches the node tip → authoritative {:ok, 7}.
        apply_fun: fn _p, _a ->
          Agent.update(calls, &(&1 + 1))
          Agent.update(local, fn _ -> 7 end)
          {:ok, 7}
        end
      )

    state = drain(pid)

    assert state.machine.phase == :synced
    # :synced from the result ⇒ no self-scheduled replay: exactly one apply.
    assert Agent.get(calls, & &1) == 1
  end

  test "a {:partial, last_height, _} that advanced folds to :progressed and replays to the tip" do
    {:ok, agent} = Agent.start_link(fn -> 80 end)
    node_hashes = for h <- 0..100, into: %{}, do: {h, hex(h)}

    pid =
      start_controller(
        rpc_height: fn -> {:ok, 100} end,
        rpc_hash_at: fn h -> node_hashes[h] end,
        local_height: fn -> Agent.get(agent, & &1) end,
        local_hash_at: fn h ->
          if h <= Agent.get(agent, & &1), do: node_hashes[h], else: nil
        end,
        # A prefix applies (height advances) but the batch trails the tip → {:partial}
        # while below 100, {:ok, 100} once caught up. A partial that *advanced* must
        # still schedule a replay.
        apply_fun: fn _p, arg ->
          Agent.update(agent, &(&1 + length(arg.connect)))
          h = Agent.get(agent, & &1)
          if h >= 100, do: {:ok, h}, else: {:partial, h, :rpc_block_unavailable}
        end,
        batch: 10
      )

    GenServer.cast(pid, {:hint, :p2p})
    state = drain(pid)

    assert Agent.get(agent, & &1) == 100
    assert state.machine.phase == :synced
  end

  test "a rollback-only partial with no forward progress folds to :deferred without spinning" do
    node_hashes = %{5 => hex(5), 6 => hex(6), 7 => hex(7)}
    {:ok, calls} = Agent.start_link(fn -> 0 end)

    pid =
      start_controller(
        rpc_height: fn -> {:ok, 7} end,
        rpc_hash_at: fn h -> node_hashes[h] end,
        local_height: fn -> 5 end,
        local_hash_at: fn h -> if(h == 5, do: hex(5), else: nil) end,
        # The connect failed on its first block: a rollback/no-prefix partial reports
        # `last_height` at-or-below where we started. No forward progress ⇒ no replay.
        apply_fun: fn _p, _a ->
          Agent.update(calls, &(&1 + 1))
          {:partial, 5, :connect_failed}
        end
      )

    # The startup continue is the single trigger; a no-forward-progress partial must
    # fold to :deferred and NOT self-schedule, so the apply runs exactly once (a spin
    # would re-fire until the drain bound).
    state = drain(pid)

    refute state.machine.in_flight
    assert Agent.get(calls, & &1) == 1
    assert state.machine.phase == :bootstrapping
  end
end
