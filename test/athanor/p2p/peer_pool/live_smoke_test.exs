defmodule Athanor.P2P.PeerPool.LiveSmokeTest do
  @moduledoc """
  Live smoke test for the peer pool against the real network (T2.7).

  Tagged `:external` and excluded from `mix test` (see T1.S); run with:

      mix test --only external

  Starts a real `Athanor.P2P.Supervisor` (DNS resolution + real `Transport.Gen`
  peers) and asserts the pool bootstraps to at least a few live peers with no
  duplicate /24s — the end-to-end proof that discovery, dialing, the handshake,
  and diversity all work against live nodes. Testnet by default;
  `P2P_SMOKE_NETWORK=mainnet` runs the mainnet variant.
  """
  use ExUnit.Case, async: false

  alias Athanor.P2P.{Network, PeerPool, PeerRegistry}
  alias Athanor.P2P.Messages.Version

  @target 5

  defp pick_network do
    case System.get_env("P2P_SMOKE_NETWORK") do
      "mainnet" -> Network.mainnet()
      _ -> Network.testnet()
    end
  end

  defp our_version do
    na = Version.net_addr(0, <<0::128>>, 0)

    %Version{
      addr_recv: na,
      addr_from: na,
      nonce: 1,
      user_agent: "/Athanor:0.1.0/",
      start_height: 0
    }
  end

  defp eventually(fun, timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_eventually(fun, deadline)
  end

  defp do_eventually(fun, deadline) do
    cond do
      fun.() ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        false

      true ->
        receive do
        after
          200 -> :ok
        end

        do_eventually(fun, deadline)
    end
  end

  @tag :external
  @tag timeout: 90_000
  test "bootstraps to several live peers with distinct /24s" do
    config = %PeerPool.Config{
      network: pick_network(),
      target: @target,
      our_version: our_version()
    }

    start_supervised!({Athanor.P2P.Supervisor, pool_config: config})

    want = min(@target, 3)

    assert eventually(fn -> length(PeerRegistry.addresses()) >= want end, 45_000),
           "pool did not reach #{want} live peers in time"

    addrs = PeerRegistry.addresses()
    # Diversity holds on the real network: one /24 per live peer.
    assert MapSet.size(PeerRegistry.slash24s()) == length(addrs)
  end
end
