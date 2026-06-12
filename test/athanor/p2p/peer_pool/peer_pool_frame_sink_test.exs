defmodule Athanor.P2P.PeerPoolFrameSinkTest do
  @moduledoc """
  Tests the pool's frame-routing seam (Phase 3, T3.S/§C): when
  `PeerPool.Config.frame_sink` is set, the pool forwards each post-handshake
  application frame to that pid as `{:peer, pid, :frame, %Frame{}}` (so the
  mempool observer receives the inbound stream with the originating peer pid),
  while still owning lifecycle and absorbing `addr` gossip. With no sink
  (default), behavior is unchanged.

  `async: false` (singleton `PeerRegistry`).
  """
  use ExUnit.Case, async: false

  alias Athanor.P2P.{Frame, Network, PeerRegistry}
  alias Athanor.P2P.Messages.Version
  alias Athanor.P2P.PeerPool

  defp pub(c), do: {{1, 0, c, 1}, 18_333}

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

  defp holder, do: spawn(fn -> receive do: (:stop -> :ok) end)

  defp start_pool(opts) do
    test = self()
    start_supervised!({PeerRegistry, [name: PeerRegistry]})

    starter = fn config ->
      p = holder()
      send(test, {:dialed, config, p})
      {:ok, p}
    end

    config =
      struct!(
        %PeerPool.Config{
          network: Network.testnet(),
          target: 1,
          our_version: ver(),
          peer_starter: starter,
          resolver: fn _ -> {:error, :nxdomain} end,
          seeds: [pub(1)],
          now_fun: fn -> 0 end
        },
        opts
      )

    start_supervised!({PeerPool, config})
  end

  test "forwards post-handshake frames to the configured frame_sink with the peer pid" do
    pool = start_pool(frame_sink: self())
    assert_receive {:dialed, _cfg, pid}
    send(pool, {:peer, pid, :ready, ver()})
    _ = :sys.get_state(pool)

    inv = Frame.encode(Network.testnet(), :inv, <<1, 0::256>>)
    send(pool, {:peer, pid, :frame, %Frame{command: "inv", payload: <<1, 0::256>>}})
    _ = inv

    assert_receive {:peer, ^pid, :frame, %Frame{command: "inv"}}
  end

  test "fans a frame out to every sink when frame_sink is a list (Phase 4 §A)" do
    test = self()
    relay = spawn(fn -> receive do: (m -> send(test, {:relayed, m})) end)
    pool = start_pool(frame_sink: [self(), relay])
    assert_receive {:dialed, _cfg, pid}
    send(pool, {:peer, pid, :ready, ver()})
    _ = :sys.get_state(pool)

    send(pool, {:peer, pid, :frame, %Frame{command: "inv", payload: <<>>}})

    # Both sinks receive the same forwarded message.
    assert_receive {:peer, ^pid, :frame, %Frame{command: "inv"}}
    assert_receive {:relayed, {:peer, ^pid, :frame, %Frame{command: "inv"}}}
  end

  test "does not forward when no frame_sink is configured (default)" do
    pool = start_pool([])
    assert_receive {:dialed, _cfg, pid}
    send(pool, {:peer, pid, :ready, ver()})
    _ = :sys.get_state(pool)

    send(pool, {:peer, pid, :frame, %Frame{command: "inv", payload: <<>>}})
    _ = :sys.get_state(pool)

    refute_received {:peer, ^pid, :frame, _}
  end
end
