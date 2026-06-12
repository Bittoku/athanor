defmodule Athanor.P2P.TxFetcher.IntegrationTest do
  @moduledoc """
  Real-socket end-to-end for the Phase 5 §B raw-tx pull-fetch (T5.5): a
  `B2gResolver` parent-fetch resolves over loopback through the real
  `P2P.Supervisor` + a `FakePeerServer` holding a tx in its "mempool" (answers our
  unsolicited `getdata` with that `tx`, and `notfound` for any other txid).

    * a parent the peer holds is fetched via P2P, and the REST/RPC providers are
      **never called** (injected flunking stubs); and
    * a parent the peer does **not** hold gets a `notfound` → the fetcher misses →
      the resolve falls through to the REST/RPC cascade.

  The cold-start case (zero peers → REST immediately) is covered deterministically
  at the unit level (`b2g_resolver_routing_test.exs`); here we prove the live
  `getdata → tx`/`notfound` exchange. `async: false` (real sockets + singleton
  registry + SQL sandbox); a bounded `eventually/1` is the one allowed
  real-process reality check.
  """
  use Athanor.DataCase, async: false

  alias Athanor.Indexer.B2gResolver
  alias Athanor.P2P.{FakePeerServer, Network, PeerPool, PeerRegistry, TxFetcher}
  alias Athanor.P2P.Messages.Version
  alias Athanor.P2P.Transport.LoopbackRewrite

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

  defp p2pkh_tx(byte) do
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
        %BSV.Transaction.Output{
          satoshis: 1000,
          locking_script: BSV.Script.p2pkh_lock(:binary.copy(<<byte>>, 20))
        }
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
      res = fun.() ->
        res

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

  # A provider seam that drives the real supervised TxFetcher for `:p2p`.
  defp real_p2p(txid_hex) do
    {:ok, wire} = Base.decode16(txid_hex, case: :mixed)

    case TxFetcher.fetch(TxFetcher, wire) do
      {:ok, raw} ->
        {:ok, tx, _rest} = BSV.Transaction.from_binary(raw)
        {:ok, tx}

      :miss ->
        :miss
    end
  end

  defp start_p2p(served_tx) do
    net = Network.testnet()

    {:ok, port, _pid} =
      FakePeerServer.start(
        network: net,
        report_to: self(),
        peer_version: ver(),
        announce_on_verack: false,
        tx_payload: BSV.Transaction.to_binary(served_tx),
        serve_txid: BSV.Transaction.txid_binary(served_tx)
      )

    syn = {{10, 0, 1, 1}, 18_333}
    rewrite = %{:inet.ntoa(elem(syn, 0)) => {~c"127.0.0.1", port}}

    config = %PeerPool.Config{
      network: net,
      target: 1,
      our_version: ver(),
      transport: LoopbackRewrite,
      transport_opts: [rewrite: rewrite],
      resolver: fn _ -> {:error, :nxdomain} end,
      seeds: [syn]
    }

    start_supervised!({Athanor.P2P.Supervisor, pool_config: config})
    eventually(fn -> length(PeerRegistry.pids(PeerRegistry)) == 1 end)
  end

  test "a parent in a peer's mempool resolves via P2P; REST/RPC is never called" do
    parent = p2pkh_tx(0x71)
    start_p2p(parent)

    providers = %{
      p2p: &real_p2p/1,
      rpc: fn _ -> flunk("RPC must not be called on a P2P mempool hit") end,
      junglebus: fn _ -> flunk("JungleBus must not be called on a P2P hit") end,
      whatsonchain: fn _ -> flunk("WhatsOnChain must not be called on a P2P hit") end
    }

    txid = BSV.Transaction.txid_binary(parent)

    assert {:ok, [{_hex, 0}]} =
             B2gResolver.resolve(txid, 0, providers: providers, p2p_available?: true)
  end

  test "a parent the peer lacks gets notfound → fetcher misses → REST fallback resolves it" do
    served = p2pkh_tx(0x72)
    absent = p2pkh_tx(0x73)
    start_p2p(served)

    test = self()

    providers = %{
      p2p: fn hex -> send(test, :p2p_tried) && real_p2p(hex) end,
      rpc: fn _ -> send(test, :rpc_tried) && {:ok, absent} end,
      junglebus: fn _ -> flunk("JungleBus should not be reached (RPC resolves)") end,
      whatsonchain: fn _ -> flunk("WhatsOnChain should not be reached (RPC resolves)") end
    }

    txid = BSV.Transaction.txid_binary(absent)

    assert {:ok, [{_hex, 0}]} =
             B2gResolver.resolve(txid, 0, providers: providers, p2p_available?: true)

    assert_received :p2p_tried
    assert_received :rpc_tried
  end
end
