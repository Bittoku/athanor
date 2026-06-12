defmodule Athanor.P2P.TxRelay.LiveSmokeTest do
  @moduledoc """
  Live smoke for the outbound broadcast/relay-back path against the real network
  (Phase 4 T4.4).

  Tagged `:external` and excluded from `mix test`; run with:

      mix test --only external

  Starts a real `Athanor.P2P.Supervisor` (DNS discovery + real `Transport.Gen`
  peers) on testnet, with the relay's audit sink redirected to this test process
  (so the live wire path is asserted directly, without the DB — the audit-row
  integration is covered deterministically in T4.2/T4.3). It proves:

    * the pool bootstraps to live peers (discovery → dial → handshake), and
    * if a funded raw tx hex is supplied via `P2P_SMOKE_RAW_TX`, broadcasting it
      announces `inv` to the chosen targets and our **own txid comes back** via a
      non-target peer (the network accepted and re-gossiped it) within the
      window, surfacing as a `:propagated` relay event.

  Testnet by default; `P2P_SMOKE_NETWORK=mainnet` opts in to mainnet. Requires a
  spendable, fully-signed tx the network will accept (otherwise peers `reject`
  it and it never propagates).
  """
  use ExUnit.Case, async: false

  alias Athanor.P2P.{Network, PeerPool, PeerRegistry, TxRelay}
  alias Athanor.P2P.Messages.Version

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
  @tag timeout: 240_000
  test "broadcasts a tx to live peers and sees it relayed back (propagated)" do
    test = self()

    config = %PeerPool.Config{
      network: pick_network(),
      target: 5,
      our_version: our_version()
    }

    # Redirect the relay's audit to this test so we observe the live lifecycle
    # without the DB (the audit-row bridge is proven in T4.2/T4.3).
    start_supervised!(
      {Athanor.P2P.Supervisor,
       pool_config: config, relay_opts: [audit: fn event -> send(test, {:relay_event, event}) end]}
    )

    # Bootstrap proof: discovery → dial → handshake against live nodes. ≥2 peers
    # are needed for a reachable propagation bar (`bar = min(2, N−1) ≥ 1`).
    assert eventually(fn -> length(PeerRegistry.pids()) >= 2 end, 90_000),
           "pool did not reach two live peers in time"

    case System.get_env("P2P_SMOKE_RAW_TX") do
      nil ->
        # No funded tx supplied: the bootstrap above proves discovery/dial/
        # handshake. Provide a spendable signed tx via P2P_SMOKE_RAW_TX to
        # exercise the announce → relay-back → propagated path.
        :ok

      raw_hex ->
        {:ok, tx} = BSV.Transaction.from_hex(raw_hex)
        txid_bin = BSV.Transaction.txid_binary(tx)
        raw_bin = BSV.Transaction.to_binary(tx)

        assert :ok = TxRelay.broadcast(txid_bin, raw_bin)
        # Our own tx comes back from a peer we did NOT announce to → propagated.
        assert_receive {:relay_event, {:propagated, ^txid_bin}}, 180_000
    end
  end
end
