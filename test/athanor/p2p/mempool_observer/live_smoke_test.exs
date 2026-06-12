defmodule Athanor.P2P.MempoolObserver.LiveSmokeTest do
  @moduledoc """
  Live smoke for the mempool observer against the real network (Phase 3 T3.6).

  Tagged `:external` and excluded from `mix test`; run with:

      mix test --only external

  Starts a real `Athanor.P2P.Supervisor` (DNS discovery + real `Transport.Gen`
  peers) on testnet with the observer wired as the pool's `frame_sink`, and
  proves the P2P ingest path against live nodes:

    * the pool bootstraps to at least one live peer (discovery → dial →
      handshake), and
    * if a funded watched address is supplied via `P2P_SMOKE_ADDRESS`, a real
      mempool payment to it is observed via `inv → getdata → tx`, verified, and
      delivered to the pipeline within the window.

  Testnet by default; `P2P_SMOKE_NETWORK=mainnet` opts in to mainnet. This is the
  one true external dependency in the suite.
  """
  use ExUnit.Case, async: false

  alias Athanor.P2P.{Network, PeerPool, PeerRegistry, Watchlist}
  alias Athanor.P2P.Messages.Version
  alias Athanor.P2P.MempoolObserver

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
        true

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
  @tag timeout: 180_000
  test "observes the P2P mempool path against live peers" do
    test = self()
    address = System.get_env("P2P_SMOKE_ADDRESS")

    watchlist =
      case address do
        nil -> Watchlist.new()
        addr -> Watchlist.put_address(Watchlist.new(), addr)
      end

    observer =
      start_supervised!(
        {
          MempoolObserver,
          # Capture any ingested tx so the operator can confirm the live path.
          watchlist: watchlist,
          matcher: fn _tx -> {(address && [address]) || ["smoke"], []} end,
          pipeline: fn tx, addrs, _tokens, source ->
            send(test, {:smoke_ingested, BSV.Transaction.txid_binary(tx), addrs, source})
          end
        },
        id: {MempoolObserver, make_ref()}
      )

    config = %PeerPool.Config{
      network: pick_network(),
      target: 5,
      our_version: our_version(),
      frame_sink: observer
    }

    start_supervised!({Athanor.P2P.Supervisor, pool_config: config})

    # Bootstrap proof: discovery → dial → handshake against live nodes.
    assert eventually(fn -> length(PeerRegistry.addresses()) >= 1 end, 60_000),
           "pool did not reach a live peer in time"

    if address do
      # A real payment to the watched address should land via the P2P path.
      assert_receive {:smoke_ingested, _txid, _addrs, :p2p}, 120_000
    else
      # No funded address supplied: the bootstrap above already proves
      # discovery/dial/handshake; fund + set P2P_SMOKE_ADDRESS to exercise ingest.
      :ok
    end
  end
end
