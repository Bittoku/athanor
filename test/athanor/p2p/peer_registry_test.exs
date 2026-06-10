defmodule Athanor.P2P.PeerRegistryTest do
  @moduledoc """
  Tests for `Athanor.P2P.PeerRegistry` (T2.1) — a monitor-backed view of live
  peers keyed by `{ip, port}`. The critical property is that a peer is removed
  when its **actual process dies** (not merely on explicit unregister), proven
  here by killing a registered process and asserting it disappears. No sleeps:
  death is observed with `Process.monitor` + `assert_receive {:DOWN, ...}`.
  """
  use ExUnit.Case, async: true

  alias Athanor.P2P.PeerRegistry

  defp addr(c), do: {{10, 0, c, 1}, 18_333}

  # A process that stays alive until told to stop (no Process.sleep).
  defp holder do
    spawn(fn ->
      receive do
        :stop -> :ok
      end
    end)
  end

  setup do
    %{reg: start_supervised!(PeerRegistry)}
  end

  test "register / lookup / unregister round-trip", %{reg: reg} do
    a = addr(1)
    p = holder()

    assert :ok = PeerRegistry.register(reg, a, p)
    assert {:ok, ^p} = PeerRegistry.lookup(reg, a)
    assert PeerRegistry.addresses(reg) == [a]
    assert PeerRegistry.slash24s(reg) == MapSet.new([{10, 0, 1}])

    assert :ok = PeerRegistry.unregister(reg, a)
    assert :error = PeerRegistry.lookup(reg, a)
    assert PeerRegistry.addresses(reg) == []

    send(p, :stop)
  end

  test "a second register for a taken address is rejected (unique keys)", %{reg: reg} do
    a = addr(1)
    p1 = holder()
    p2 = holder()

    assert :ok = PeerRegistry.register(reg, a, p1)
    assert {:error, :already_registered} = PeerRegistry.register(reg, a, p2)

    send(p1, :stop)
    send(p2, :stop)
  end

  test "removes a peer when its process dies (monitor-driven cleanup)", %{reg: reg} do
    a = addr(2)
    p = holder()
    assert :ok = PeerRegistry.register(reg, a, p)

    ref = Process.monitor(p)
    send(p, :stop)
    assert_receive {:DOWN, ^ref, :process, ^p, _reason}

    # The registry's own monitor DOWN was enqueued at the same death, ahead of
    # this synchronous lookup, so the entry must already be gone.
    assert :error = PeerRegistry.lookup(reg, a)
    assert PeerRegistry.addresses(reg) == []
    assert PeerRegistry.slash24s(reg) == MapSet.new()
  end

  test "tracks multiple peers and their /24s", %{reg: reg} do
    a1 = addr(1)
    a2 = addr(2)
    p1 = holder()
    p2 = holder()

    :ok = PeerRegistry.register(reg, a1, p1)
    :ok = PeerRegistry.register(reg, a2, p2)

    assert Enum.sort(PeerRegistry.addresses(reg)) == Enum.sort([a1, a2])
    assert PeerRegistry.slash24s(reg) == MapSet.new([{10, 0, 1}, {10, 0, 2}])

    send(p1, :stop)
    send(p2, :stop)
  end
end
