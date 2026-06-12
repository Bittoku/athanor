defmodule Athanor.P2P.TxRelay.IntegrationTest do
  @moduledoc """
  Real-socket end-to-end for the outbound broadcast path (Phase 4 T4.3): a
  self-broadcast round-trips over loopback through the real `P2P.Supervisor`
  (Registry → Observer → TxRelay → Pool) against **three** `FakePeerServer`s, so
  the hold-back rule yields genuine non-targets (`held = bar = min(2, N−1) = 2`;
  announce to `N−2 = 1`, hold back 2).

  Flow:
    * `broadcast_tx` (P2P-primary, `rpc_fallback?: false`) → the relay announces
      `inv(our_txid)` to the one announce target;
    * the **announce target** (`serve_on_inv: true`) answers with `getdata`, we
      serve the `tx`, and the server reports `{:server_received, :tx, _}` —
      proving the announce → getdata → serve exchange over a real socket;
    * the test then commands **all** servers to relay the tx back
      (`{:cmd, :relay_back}` → `inv(our_txid)`); the two held-back peers are
      counted (peer ∉ `announced_to`) while the one target's echo is ignored, so
      the relay crosses the bar and marks the broadcast `:propagated`, lifting the
      audit row to `propagated`.

  The RPC fallback is opted out (`rpc_fallback?: false`) with a `:broadcaster`
  that flunks if called — so `:propagated` is proven to come through the P2P
  announce → non-target relay-back path, **not** the node/RPC path.

  `async: false` (real sockets + singleton registry + SQL sandbox). A bounded
  `eventually/1` poll is the one allowed real-process reality check (handshake +
  broadcast + relay-back complete asynchronously).
  """
  use Athanor.DataCase, async: false

  alias Athanor.P2P.{FakePeerServer, Network, PeerPool, PeerRegistry}
  alias Athanor.P2P.Messages.Version
  alias Athanor.P2P.Transport.LoopbackRewrite
  alias Athanor.Repo
  alias Athanor.Schema.Broadcast
  alias Athanor.Services.Broadcast, as: BroadcastService

  defp ver do
    na = Version.net_addr(0, <<0::128>>, 0)

    %Version{
      addr_recv: na,
      addr_from: na,
      nonce: 1,
      user_agent: "/Bitcoin SV:1.2.2/",
      start_height: 1
    }
  end

  defp p2pkh_tx(pkh) do
    %BSV.Transaction{
      version: 1,
      inputs: [
        %BSV.Transaction.Input{
          source_txid: :crypto.strong_rand_bytes(32),
          source_tx_out_index: 0,
          unlocking_script: %BSV.Script{chunks: []},
          sequence_number: 0xFFFFFFFF
        }
      ],
      outputs: [
        %BSV.Transaction.Output{satoshis: 1000, locking_script: BSV.Script.p2pkh_lock(pkh)}
      ],
      lock_time: 0
    }
  end

  defp eventually(fun, timeout \\ 4_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_eventually(fun, deadline)
  end

  defp do_eventually(fun, deadline) do
    cond do
      result = fun.() ->
        result

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("condition not met within timeout")

      true ->
        receive do
        after
          25 -> :ok
        end

        do_eventually(fun, deadline)
    end
  end

  test "a self-broadcast round-trips and is marked propagated via held-back, non-target relay-backs" do
    net = Network.testnet()

    tx = p2pkh_tx(:binary.copy(<<0x71>>, 20))
    hex = BSV.Transaction.to_hex(tx)
    raw_bin = BSV.Transaction.to_binary(tx)
    txid_bin = BSV.Transaction.txid_binary(tx)
    txid_hex = BSV.Transaction.tx_id_hex(tx)

    # Three peers: each stays quiet after the handshake (so the only inv it emits
    # is the explicit relay-back), serves our tx on a getdata, and relays the tx
    # back on command. The selector decides which one is the announce target; the
    # other two are non-target relay-backs — the outcome is identical regardless.
    servers =
      for i <- 1..3 do
        {:ok, port, pid} =
          FakePeerServer.start(
            network: net,
            report_to: self(),
            peer_version: ver(),
            announce_on_verack: false,
            serve_on_inv: true,
            tx_payload: raw_bin,
            relay_back_hash: txid_bin
          )

        {{{10, 0, i, 1}, 18_333}, port, pid}
      end

    rewrite =
      Map.new(servers, fn {syn, port, _} ->
        {:inet.ntoa(elem(syn, 0)), {~c"127.0.0.1", port}}
      end)

    config = %PeerPool.Config{
      network: net,
      target: 3,
      our_version: ver(),
      transport: LoopbackRewrite,
      transport_opts: [rewrite: rewrite],
      resolver: fn _ -> {:error, :nxdomain} end,
      seeds: Enum.map(servers, fn {syn, _, _} -> syn end)
    }

    start_supervised!({Athanor.P2P.Supervisor, pool_config: config})

    # All three peers dialed, handshaked, and registered.
    eventually(fn -> length(PeerRegistry.pids(PeerRegistry)) == 3 end)

    # Broadcast: P2P-primary, RPC fallback opted out (and flunking if invoked) so
    # propagation can only come through the P2P relay-back path.
    {:ok, row} =
      BroadcastService.broadcast_tx(hex,
        peers_available?: true,
        rpc_fallback?: false,
        broadcaster: fn _ -> flunk("RPC fallback must not run on the propagation path") end
      )

    assert row.status == "relayed"
    assert row.txid == txid_hex

    # The announce target pulled our tx over the wire (getdata → serve).
    assert_receive {:server_received, :tx, _bytes}, 4_000

    # Now have every peer advertise the tx back. The two held-back peers (∉
    # announced_to) are counted; the one target's echo is ignored — so reaching
    # the bar proves propagation came from non-targets.
    Enum.each(servers, fn {_syn, _port, pid} -> send(pid, {:cmd, :relay_back}) end)

    row = eventually(fn -> Repo.get_by(Broadcast, txid: txid_hex) end)
    assert eventually(fn -> Repo.reload(row).status == "propagated" end)
  end
end
