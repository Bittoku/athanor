defmodule Athanor.P2P.TxFetcherTest do
  @moduledoc """
  Tests for `Athanor.P2P.TxFetcher` (Phase 5 T5.2, §B) — the thin GenServer that
  pulls a specific tx from the peer set via `getdata`. `fetch/3` is synchronous;
  its reply is **held** until the pure `Tracker` resolves `{:ok, raw}` (a matching
  `tx` from an asked peer) or `:miss` (every asked peer `notfound`s, the timeout
  fires, or there are zero peers).

  Real `:ready` `Peer`s over `Transport.Fake` (so emitted `getdata` bytes are
  genuinely captured); a deterministic `:selector`; a clock `Agent` for `now_fun`
  so the timeout is driven without sleeping. The blocking `fetch/3` runs in a
  `Task`; `wait_until/1` polls the server's own state to synchronize before
  delivering the resolving frame. `async: false` (singleton `PeerRegistry`).
  """
  use ExUnit.Case, async: false

  alias Athanor.P2P.Codec.VarInt
  alias Athanor.P2P.{Frame, Network, Peer, PeerRegistry, TxFetcher}
  alias Athanor.P2P.Messages.{Inv, Version}
  alias Athanor.P2P.Transport.Fake

  @net Network.mainnet()

  defp ready_peer do
    na = Version.net_addr(0, <<0::128>>, 0)

    our = %Version{
      addr_recv: na,
      addr_from: na,
      nonce: 1,
      user_agent: "/Athanor:0.1.0/",
      start_height: 0
    }

    peer = %Version{
      addr_recv: na,
      addr_from: na,
      nonce: 2,
      user_agent: "/Bitcoin SV:1.2.2/",
      start_height: 1
    }

    config = %Peer.Config{
      host: ~c"127.0.0.1",
      port: 8333,
      network: @net,
      our_version: our,
      transport: Fake,
      transport_opts: [fake: %{test: self()}],
      owner: self(),
      timeouts: %{handshake: 1_000}
    }

    pid = start_supervised!({Peer, config}, restart: :temporary, id: {Peer, make_ref()})
    assert_receive {:fake_handle, socket}
    :ok = Fake.deliver(socket, Frame.encode(@net, :version, Version.serialize(peer)))
    :ok = Fake.deliver(socket, Frame.encode(@net, :verack, <<>>))
    assert_receive {:peer, ^pid, :ready, _}
    {pid, socket}
  end

  defp p2pkh_tx(byte) do
    tx = %BSV.Transaction{
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

    {BSV.Transaction.txid_binary(tx), BSV.Transaction.to_binary(tx)}
  end

  defp setup_registry, do: start_supervised!({PeerRegistry, name: PeerRegistry})

  defp register(pid, octet),
    do: (:ok = PeerRegistry.register(PeerRegistry, {{10, 0, octet, 1}, 8333}, pid)) && pid

  defp start_fetcher(opts) do
    {:ok, clock} = start_supervised({Agent, fn -> 0 end}, id: {Agent, make_ref()})

    defaults = [
      registry: PeerRegistry,
      now_fun: fn -> Agent.get(clock, & &1) end,
      tick_interval_ms: 60_000,
      timeout_ms: 100
    ]

    fetcher =
      start_supervised!({TxFetcher, Keyword.merge(defaults, opts)}, id: {TxFetcher, make_ref()})

    {fetcher, clock}
  end

  defp fetch_async(fetcher, txid) do
    test = self()
    spawn(fn -> send(test, {:fetched, txid, TxFetcher.fetch(fetcher, txid)}) end)
  end

  defp wait_until(fun, tries \\ 200) do
    cond do
      fun.() ->
        :ok

      tries == 0 ->
        flunk("condition not met")

      true ->
        receive do
        after
          5 -> :ok
        end

        wait_until(fun, tries - 1)
    end
  end

  defp pending?(fetcher, txid), do: Map.has_key?(:sys.get_state(fetcher).tracker.pending, txid)

  test "zero peers → :miss immediately with no getdata (cold-start gate)" do
    setup_registry()
    {fetcher, _} = start_fetcher([])
    {txid, _raw} = p2pkh_tx(0x41)
    assert TxFetcher.fetch(fetcher, txid) == :miss
  end

  test "fetch getdatas the selector-chosen peers and resolves {:ok, raw} on a matching tx" do
    setup_registry()
    {p, sock} = ready_peer()
    register(p, 1)
    {fetcher, _} = start_fetcher(selector: fn _pids, _fanout -> [p] end)

    {txid, raw} = p2pkh_tx(0x42)
    fetch_async(fetcher, txid)
    wait_until(fn -> pending?(fetcher, txid) end)

    _ = :sys.get_state(p)
    assert Frame.encode(@net, :getdata, Inv.serialize([{:tx, txid}])) in Fake.sent(sock)

    send(fetcher, {:peer, p, :frame, %Frame{command: "tx", payload: raw}})
    assert_receive {:fetched, ^txid, {:ok, ^raw}}, 1_000
  end

  test "every asked peer notfound → :miss" do
    setup_registry()
    {p, _} = ready_peer()
    register(p, 1)
    {fetcher, _} = start_fetcher(selector: fn _pids, _fanout -> [p] end)

    {txid, _raw} = p2pkh_tx(0x43)
    fetch_async(fetcher, txid)
    wait_until(fn -> pending?(fetcher, txid) end)

    send(
      fetcher,
      {:peer, p, :frame, %Frame{command: "notfound", payload: Inv.serialize([{:tx, txid}])}}
    )

    assert_receive {:fetched, ^txid, :miss}, 1_000
  end

  test "timeout (driven via the clock + :tick) → :miss" do
    setup_registry()
    {p, _} = ready_peer()
    register(p, 1)
    {fetcher, clock} = start_fetcher(selector: fn _pids, _fanout -> [p] end, timeout_ms: 100)

    {txid, _raw} = p2pkh_tx(0x44)
    fetch_async(fetcher, txid)
    wait_until(fn -> pending?(fetcher, txid) end)

    Agent.update(clock, fn _ -> 101 end)
    send(fetcher, :tick)
    assert_receive {:fetched, ^txid, :miss}, 1_000
  end

  test "a malformed tx frame is dropped (no resolve); the request still times out to :miss" do
    setup_registry()
    {p, _} = ready_peer()
    register(p, 1)
    {fetcher, clock} = start_fetcher(selector: fn _pids, _fanout -> [p] end, timeout_ms: 100)

    {txid, _raw} = p2pkh_tx(0x45)
    fetch_async(fetcher, txid)
    wait_until(fn -> pending?(fetcher, txid) end)

    # Un-parseable tx payload → dropped before the reducer (no resolve).
    send(fetcher, {:peer, p, :frame, %Frame{command: "tx", payload: VarInt.write(0xFFFFFFFF)}})
    refute_received {:fetched, ^txid, _}

    Agent.update(clock, fn _ -> 101 end)
    send(fetcher, :tick)
    assert_receive {:fetched, ^txid, :miss}, 1_000
  end
end
