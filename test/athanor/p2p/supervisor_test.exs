defmodule Athanor.P2P.SupervisorTest do
  @moduledoc """
  Tests for `Athanor.P2P.Supervisor` (T2.5): the config-gated tree that owns the
  `PeerRegistry` and `PeerPool`. Verifies child membership, `:rest_for_one`
  restart semantics (registry restart takes the pool with it, not vice-versa),
  the `enabled?/0` config gate (default off), and that the app tree excludes the
  P2P supervisor by default.

  `async: false` (singleton `PeerRegistry` name + global app env).
  """
  use ExUnit.Case, async: false

  alias Athanor.P2P.{Network, PeerPool, PeerRegistry}
  alias Athanor.P2P.Messages.Version

  defp ver do
    na = Version.net_addr(0, <<0::128>>, 0)

    %Version{
      addr_recv: na,
      addr_from: na,
      nonce: 1,
      user_agent: "/Athanor:0.1.0/",
      start_height: 0
    }
  end

  defp fake_config do
    %PeerPool.Config{
      network: Network.testnet(),
      target: 2,
      our_version: ver(),
      peer_starter: fn _ -> {:ok, spawn(fn -> receive do: (:stop -> :ok) end)} end,
      resolver: fn _ -> {:error, :nxdomain} end,
      seeds: [],
      now_fun: fn -> 0 end
    }
  end

  defp child_pid(sup, id) do
    case Enum.find(Supervisor.which_children(sup), fn {cid, _, _, _} -> cid == id end) do
      {^id, pid, _, _} when is_pid(pid) -> pid
      _ -> nil
    end
  end

  test "supervises a PeerRegistry and a PeerPool" do
    sup = start_supervised!({Athanor.P2P.Supervisor, [pool_config: fake_config()]})

    ids = Supervisor.which_children(sup) |> Enum.map(&elem(&1, 0))
    assert PeerRegistry in ids
    assert PeerPool in ids
  end

  test "killing the pool restarts only the pool" do
    sup = start_supervised!({Athanor.P2P.Supervisor, [pool_config: fake_config()]})
    reg1 = child_pid(sup, PeerRegistry)
    pool1 = child_pid(sup, PeerPool)

    ref = Process.monitor(pool1)
    Process.exit(pool1, :kill)
    assert_receive {:DOWN, ^ref, :process, ^pool1, _}

    # The supervisor processed the child exit (enqueued before this call) and
    # restarted the pool; the registry is untouched.
    assert child_pid(sup, PeerRegistry) == reg1
    pool2 = child_pid(sup, PeerPool)
    assert is_pid(pool2) and pool2 != pool1
  end

  test "killing the registry restarts both (rest_for_one)" do
    sup = start_supervised!({Athanor.P2P.Supervisor, [pool_config: fake_config()]})
    reg1 = child_pid(sup, PeerRegistry)
    pool1 = child_pid(sup, PeerPool)

    ref = Process.monitor(reg1)
    Process.exit(reg1, :kill)
    assert_receive {:DOWN, ^ref, :process, ^reg1, _}

    # rest_for_one: the registry comes first, so its restart also restarts the
    # pool that was started after it.
    reg2 = child_pid(sup, PeerRegistry)
    pool2 = child_pid(sup, PeerPool)
    assert is_pid(reg2) and reg2 != reg1
    assert is_pid(pool2) and pool2 != pool1
  end

  test "enabled?/0 defaults to false and reflects config" do
    refute Athanor.P2P.Supervisor.enabled?()

    Application.put_env(:athanor, Athanor.P2P, enabled: true)
    on_exit(fn -> Application.delete_env(:athanor, Athanor.P2P) end)
    assert Athanor.P2P.Supervisor.enabled?()
  end

  test "the app supervision tree excludes the P2P supervisor by default" do
    ids = Supervisor.which_children(Athanor.Supervisor) |> Enum.map(&elem(&1, 0))
    refute Athanor.P2P.Supervisor in ids
  end

  test "runtime_pool_config/0 builds a valid pool config from env" do
    cfg = Athanor.P2P.Supervisor.runtime_pool_config()
    assert %PeerPool.Config{network: %Network{}, our_version: %Version{}} = cfg
    assert cfg.target > 0
  end
end
