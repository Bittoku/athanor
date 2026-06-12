defmodule Athanor.P2P.PeerPoolGossipTest do
  @moduledoc """
  Tests for `addr`-gossip absorption by `Athanor.P2P.PeerPool` (T2.4). Live peers
  forward `addr` frames to the pool (their owner); the pool parses them, keeps
  the routable addresses, folds them into the address book, and dials newly
  learned peers. Non-routable / duplicate gossip and non-`addr` frames are
  ignored.

  `async: false` (singleton `PeerRegistry`).
  """
  use ExUnit.Case, async: false

  alias Athanor.P2P.{Frame, Network, PeerRegistry}
  alias Athanor.P2P.Messages.{Addr, Version}
  alias Athanor.P2P.PeerPool

  # Public (routable) test IPs — gossip absorption filters out private ranges.
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

  defp addr_frame(ips) do
    entries =
      Enum.map(ips, fn {a, b, c, d} -> {0, 0, Addr.ipv4_to_16(<<a, b, c, d>>), 18_333} end)

    %Frame{command: "addr", payload: Addr.serialize(entries)}
  end

  defp start_pool(seeds, target) do
    test = self()
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
      resolver: fn _ -> {:error, :nxdomain} end,
      seeds: seeds,
      cooldown_ms: 1_000,
      now_fun: fn -> 0 end
    }

    start_supervised!({PeerPool, config})
  end

  test "absorbs routable addr gossip and dials the newly learned peer" do
    pool = start_pool([pub(1)], 2)
    assert_receive {:dialed, _cfg1, _}
    # Only one seed candidate; the second slot stays open until we learn more.
    refute_receive {:dialed, _, _}, 50

    send(pool, {:peer, self(), :frame, addr_frame([{1, 0, 2, 1}])})

    assert_receive {:dialed, cfg2, _}
    assert {:ok, {1, 0, 2, 1}} = :inet.parse_address(cfg2.host)
  end

  test "ignores non-routable (private) gossip" do
    pool = start_pool([pub(1)], 2)
    assert_receive {:dialed, _, _}
    refute_receive {:dialed, _, _}, 50

    send(pool, {:peer, self(), :frame, addr_frame([{10, 0, 9, 1}])})
    refute_receive {:dialed, _, _}, 50
  end

  test "ignores non-addr frames and stays alive" do
    pool = start_pool([pub(1)], 2)
    assert_receive {:dialed, _, _}

    send(pool, {:peer, self(), :frame, %Frame{command: "inv", payload: <<>>}})
    refute_receive {:dialed, _, _}, 50
    # Still serving.
    assert %{} = :sys.get_state(pool)
  end
end
