defmodule Athanor.P2P.PeerPoolTest do
  @moduledoc """
  Tests for the `Athanor.P2P.PeerPool` GenServer (T2.3), driven over a fake peer
  starter (no real sockets) and an injected clock (no wall-clock dependence).

  The fake starter messages the test with each `{:dialed, peer_config, pid}` so
  the test can both inspect what was dialed and simulate the peer's lifecycle by
  sending `{:peer, pid, :ready|:down, _}` back to the pool. The pool is the
  `owner` of every peer it starts.

  `async: false` because it uses the singleton `PeerRegistry` name.
  """
  use ExUnit.Case, async: false

  alias Athanor.P2P.{Network, PeerRegistry}
  alias Athanor.P2P.Messages.Version
  alias Athanor.P2P.PeerPool

  defp a(c), do: {{10, 0, c, 1}, 18_333}
  defp slash24({a, b, c, _d}), do: {a, b, c}

  defp ver do
    na = Version.net_addr(0, <<0::128>>, 0)

    %Version{
      addr_recv: na,
      addr_from: na,
      nonce: 9,
      user_agent: "/Bitcoin SV:1.2.2/",
      start_height: 1
    }
  end

  # A process standing in for a started Peer (no real socket).
  defp holder, do: spawn(fn -> receive do: (:stop -> :ok) end)

  defp start_pool(opts) do
    test = self()
    seeds = Keyword.fetch!(opts, :seeds)
    target = Keyword.get(opts, :target, 8)
    cooldown_ms = Keyword.get(opts, :cooldown_ms, 1_000)

    {:ok, clock} = start_supervised({Agent, fn -> 0 end}, id: :clock)
    # The pool uses the named singleton registry.
    start_supervised!({PeerRegistry, [name: PeerRegistry]})

    starter = fn config ->
      p = holder()
      send(test, {:dialed, config, p})
      {:ok, p}
    end

    config = %PeerPool.Config{
      network: Network.testnet(),
      target: target,
      our_version: ver(),
      peer_starter: starter,
      resolver: fn _host -> {:error, :nxdomain} end,
      seeds: seeds,
      cooldown_ms: cooldown_ms,
      now_fun: fn -> Agent.get(clock, & &1) end
    }

    pool = start_supervised!({PeerPool, config})
    {pool, clock}
  end

  defp dialed_ip(config) do
    {:ok, ip} = :inet.parse_address(config.host)
    ip
  end

  test "dials up to target peers in distinct /24s, as owner", _ctx do
    {pool, _clock} = start_pool(seeds: [a(1), a(2), a(3), a(4)], target: 3)

    dials =
      for _ <- 1..3,
          do:
            (
              assert_receive {:dialed, cfg, _pid}
              cfg
            )

    assert Enum.all?(dials, &(&1.owner == pool))
    s24s = dials |> Enum.map(&slash24(dialed_ip(&1)))
    assert length(Enum.uniq(s24s)) == 3
    # Target reached → no fourth dial.
    refute_receive {:dialed, _, _}, 50
  end

  test "promotes a peer to live on :ready and registers it", _ctx do
    {pool, _clock} = start_pool(seeds: [a(1), a(2)], target: 1)

    assert_receive {:dialed, cfg, pid}
    ip = dialed_ip(cfg)

    send(pool, {:peer, pid, :ready, ver()})
    _ = :sys.get_state(pool)

    assert {:ok, ^pid} = PeerRegistry.lookup({ip, cfg.port})
  end

  test "self-heals: a :down frees the slot and re-dials a fresh /24", _ctx do
    {pool, _clock} = start_pool(seeds: [a(1), a(2)], target: 1)

    assert_receive {:dialed, cfg1, pid1}
    ip1 = dialed_ip(cfg1)
    send(pool, {:peer, pid1, :ready, ver()})
    _ = :sys.get_state(pool)

    # The live peer drops.
    send(pool, {:peer, pid1, :down, :closed})

    # Pool re-dials the other candidate (ip1's /24 is on cooldown).
    assert_receive {:dialed, cfg2, _pid2}
    assert slash24(dialed_ip(cfg2)) != slash24(ip1)

    # The dropped peer is gone from the registry.
    _ = :sys.get_state(pool)
    assert :error = PeerRegistry.lookup({ip1, cfg1.port})
  end

  test "honors cooldown: a dropped address is not redialed until the deadline", _ctx do
    {pool, clock} = start_pool(seeds: [a(1)], target: 1, cooldown_ms: 1_000)

    assert_receive {:dialed, _cfg, pid}
    send(pool, {:peer, pid, :ready, ver()})
    _ = :sys.get_state(pool)

    # Drops at t=0 → cooldown until t=1000. Only one candidate, so the immediate
    # refill finds nothing.
    send(pool, {:peer, pid, :down, :closed})
    refute_receive {:dialed, _, _}, 50

    # A refresh tick before the deadline still finds nothing.
    Agent.update(clock, fn _ -> 500 end)
    send(pool, :refresh)
    refute_receive {:dialed, _, _}, 50

    # After the deadline, a refresh re-dials it.
    Agent.update(clock, fn _ -> 1_000 end)
    send(pool, :refresh)
    assert_receive {:dialed, _, _}, 500
  end

  test "never dials two addresses in the same /24", _ctx do
    # Three candidates, two share /24 {10,0,1}; target 3 but only 2 distinct /24s.
    {_pool, _clock} = start_pool(seeds: [a(1), {{10, 0, 1, 2}, 18_333}, a(2)], target: 3)

    dials =
      for _ <- 1..2,
          do:
            (
              assert_receive {:dialed, cfg, _}
              cfg
            )

    s24s = dials |> Enum.map(&slash24(dialed_ip(&1)))
    assert Enum.sort(s24s) == [{10, 0, 1}, {10, 0, 2}]
    # No third dial — the third candidate collides with an occupied /24.
    refute_receive {:dialed, _, _}, 50
  end
end
