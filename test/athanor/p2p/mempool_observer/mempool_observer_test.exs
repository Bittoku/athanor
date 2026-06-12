defmodule Athanor.P2P.MempoolObserverTest do
  @moduledoc """
  Tests for `Athanor.P2P.MempoolObserver` (Phase 3 T3.3) — the thin GenServer
  shell that registers as the pool's `frame_sink`, folds inbound `inv`/`tx`/
  `notfound` frames through the pure `Tracker` (§B), requests txs via
  `Peer.send_frame/3` (§C), and on a verified+relevant `tx` runs the prefilter
  (`Watchlist`) → matcher (`TransactionFilter.matches?/1`) → pipeline with
  `source: :p2p`.

  All collaborators are injected: a real `:ready` `Peer` over `Transport.Fake`
  (so emitted `getdata` bytes are genuinely captured), a stub `matcher`, a stub
  `pipeline` sink (messages to the test), and `now_fun`/timer intervals. No
  `Process.sleep`/`Process.alive?` — synchronize via `:sys.get_state` and
  `assert_receive`.
  """
  use ExUnit.Case, async: true

  alias Athanor.P2P.{Frame, Network, Watchlist}
  alias Athanor.P2P.Messages.{Inv, Version}
  alias Athanor.P2P.{MempoolObserver, Peer}
  alias Athanor.P2P.Transport.Fake

  # ── Helpers ──

  defp ready_peer(net) do
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
      network: net,
      our_version: our,
      transport: Fake,
      transport_opts: [fake: %{test: self()}],
      owner: self(),
      timeouts: %{handshake: 1_000}
    }

    pid = start_supervised!({Peer, config}, restart: :temporary, id: {Peer, make_ref()})
    assert_receive {:fake_handle, socket}
    :ok = Fake.deliver(socket, Frame.encode(net, :version, Version.serialize(peer)))
    :ok = Fake.deliver(socket, Frame.encode(net, :verack, <<>>))
    assert_receive {:peer, ^pid, :ready, _}
    {pid, socket}
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

  defp start_observer(opts) do
    test = self()

    defaults = [
      watchlist: Watchlist.new(),
      # Default matcher: a match on one address (overridden per-test).
      matcher: fn _tx -> {["addr"], []} end,
      pipeline: fn tx, addrs, tokens, source ->
        send(test, {:ingested, tx, addrs, tokens, source})
      end,
      now_fun: fn -> 0 end,
      tick_interval_ms: 60_000,
      request_timeout_ms: 30_000
    ]

    start_supervised!({MempoolObserver, Keyword.merge(defaults, opts)},
      id: {MempoolObserver, make_ref()}
    )
  end

  defp inv_frame(txid), do: %Frame{command: "inv", payload: Inv.serialize([{:tx, txid}])}
  defp tx_frame(payload), do: %Frame{command: "tx", payload: payload}

  # ── Tests ──

  test "an inv for an unseen tx triggers a getdata to the advertising peer" do
    net = Network.mainnet()
    {peer, socket} = ready_peer(net)
    obs = start_observer([])

    txid = :binary.copy(<<0xAB>>, 32)
    send(obs, {:peer, peer, :frame, inv_frame(txid)})
    _ = :sys.get_state(obs)
    _ = :sys.get_state(peer)

    assert Frame.encode(net, :getdata, Inv.serialize([{:tx, txid}])) in Fake.sent(socket)
  end

  test "a tx for an outstanding txid is verified, prefiltered, matched, and ingested with :p2p" do
    net = Network.mainnet()
    {peer, _socket} = ready_peer(net)

    pkh = :binary.copy(<<0x51>>, 20)
    address = BSV.Base58.check_encode(pkh, 0x00)
    tx = p2pkh_tx(pkh)
    payload = BSV.Transaction.to_binary(tx)
    txid = BSV.Transaction.txid_binary(tx)

    watchlist = Watchlist.put_address(Watchlist.new(), address)
    obs = start_observer(watchlist: watchlist, matcher: fn _ -> {[address], []} end)

    # Make it outstanding, then deliver the tx.
    send(obs, {:peer, peer, :frame, inv_frame(txid)})
    _ = :sys.get_state(obs)
    send(obs, {:peer, peer, :frame, tx_frame(payload)})
    _ = :sys.get_state(obs)

    assert_receive {:ingested, ingested_tx, [^address], [], :p2p}
    assert BSV.Transaction.txid_binary(ingested_tx) == txid
  end

  test "a tx whose recomputed txid differs from the requested one is rejected (no ingest)" do
    net = Network.mainnet()
    {peer, _socket} = ready_peer(net)

    pkh = :binary.copy(<<0x52>>, 20)
    tx = p2pkh_tx(pkh)
    payload = BSV.Transaction.to_binary(tx)
    announced = :binary.copy(<<0x00>>, 32)
    refute announced == BSV.Transaction.txid_binary(tx)

    watchlist = Watchlist.put_address(Watchlist.new(), BSV.Base58.check_encode(pkh, 0x00))
    obs = start_observer(watchlist: watchlist, matcher: fn _ -> {["x"], []} end)

    # Outstanding is the *announced* (wrong) txid; the payload hashes to something else.
    send(obs, {:peer, peer, :frame, inv_frame(announced)})
    _ = :sys.get_state(obs)
    send(obs, {:peer, peer, :frame, tx_frame(payload)})
    _ = :sys.get_state(obs)

    refute_receive {:ingested, _, _, _, _}
  end

  test "a prefilter-passing but non-matching tx is dropped (no ingest)" do
    net = Network.mainnet()
    {peer, _socket} = ready_peer(net)

    pkh = :binary.copy(<<0x53>>, 20)
    tx = p2pkh_tx(pkh)
    payload = BSV.Transaction.to_binary(tx)
    txid = BSV.Transaction.txid_binary(tx)

    watchlist = Watchlist.put_address(Watchlist.new(), BSV.Base58.check_encode(pkh, 0x00))
    # matcher returns no match → drop even though the prefilter passed.
    obs = start_observer(watchlist: watchlist, matcher: fn _ -> {[], []} end)

    send(obs, {:peer, peer, :frame, inv_frame(txid)})
    _ = :sys.get_state(obs)
    send(obs, {:peer, peer, :frame, tx_frame(payload)})
    _ = :sys.get_state(obs)

    refute_receive {:ingested, _, _, _, _}
  end

  test "notfound clears the outstanding request so a later tx for it does not ingest" do
    net = Network.mainnet()
    {peer, _socket} = ready_peer(net)

    pkh = :binary.copy(<<0x54>>, 20)
    tx = p2pkh_tx(pkh)
    payload = BSV.Transaction.to_binary(tx)
    txid = BSV.Transaction.txid_binary(tx)

    watchlist = Watchlist.put_address(Watchlist.new(), BSV.Base58.check_encode(pkh, 0x00))
    obs = start_observer(watchlist: watchlist, matcher: fn _ -> {["a"], []} end)

    send(obs, {:peer, peer, :frame, inv_frame(txid)})
    _ = :sys.get_state(obs)
    # notfound for the same peer clears outstanding (not marked seen).
    send(
      obs,
      {:peer, peer, :frame, %Frame{command: "notfound", payload: Inv.serialize([{:tx, txid}])}}
    )

    _ = :sys.get_state(obs)
    # The tx now arrives, but it is no longer outstanding → ignored.
    send(obs, {:peer, peer, :frame, tx_frame(payload)})
    _ = :sys.get_state(obs)

    refute_receive {:ingested, _, _, _, _}
  end

  test "an injected per-request timeout clears the outstanding request" do
    net = Network.mainnet()
    {peer, _socket} = ready_peer(net)

    pkh = :binary.copy(<<0x55>>, 20)
    tx = p2pkh_tx(pkh)
    payload = BSV.Transaction.to_binary(tx)
    txid = BSV.Transaction.txid_binary(tx)

    watchlist = Watchlist.put_address(Watchlist.new(), BSV.Base58.check_encode(pkh, 0x00))
    obs = start_observer(watchlist: watchlist, matcher: fn _ -> {["a"], []} end)

    send(obs, {:peer, peer, :frame, inv_frame(txid)})
    _ = :sys.get_state(obs)
    # Drive the timeout directly (no sleep) — the same message the timer sends.
    send(obs, {:request_timeout, txid})
    _ = :sys.get_state(obs)
    send(obs, {:peer, peer, :frame, tx_frame(payload)})
    _ = :sys.get_state(obs)

    refute_receive {:ingested, _, _, _, _}
  end

  test "peer-down clears that peer's outstanding so another peer's inv re-requests" do
    net = Network.mainnet()
    {peer_a, _socket_a} = ready_peer(net)
    {peer_b, socket_b} = ready_peer(net)

    txid = :binary.copy(<<0xCD>>, 32)
    obs = start_observer([])

    # peerA advertises → outstanding to peerA; observer monitors peerA.
    send(obs, {:peer, peer_a, :frame, inv_frame(txid)})
    _ = :sys.get_state(obs)

    # Kill peerA; wait (via our own monitor) for the exit, then flush the
    # observer's mailbox so its :DOWN is processed before peerB's inv.
    ref = Process.monitor(peer_a)
    Process.exit(peer_a, :kill)
    assert_receive {:DOWN, ^ref, :process, ^peer_a, _}
    _ = :sys.get_state(obs)

    # peerB now advertises the same txid → re-requested (proves peer-down cleared it).
    send(obs, {:peer, peer_b, :frame, inv_frame(txid)})
    _ = :sys.get_state(obs)
    _ = :sys.get_state(peer_b)

    assert Frame.encode(net, :getdata, Inv.serialize([{:tx, txid}])) in Fake.sent(socket_b)
  end

  test "beyond the token budget, excess invs yield no getdata" do
    net = Network.mainnet()
    {peer, socket} = ready_peer(net)

    # One token only.
    obs = start_observer(tracker: [max_tokens: 1])

    txid1 = :binary.copy(<<0x01>>, 32)
    txid2 = :binary.copy(<<0x02>>, 32)
    payload = Inv.serialize([{:tx, txid1}, {:tx, txid2}])
    send(obs, {:peer, peer, :frame, %Frame{command: "inv", payload: payload}})
    _ = :sys.get_state(obs)
    _ = :sys.get_state(peer)

    getdatas =
      Enum.filter(Fake.sent(socket), fn bytes ->
        bytes == Frame.encode(net, :getdata, Inv.serialize([{:tx, txid1}])) or
          bytes == Frame.encode(net, :getdata, Inv.serialize([{:tx, txid2}]))
      end)

    assert length(getdatas) == 1
  end
end
