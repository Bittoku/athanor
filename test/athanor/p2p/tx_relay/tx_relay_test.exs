defmodule Athanor.P2P.TxRelayTest do
  @moduledoc """
  Tests for `Athanor.P2P.TxRelay` (Phase 4 T4.1) — the thin GenServer shell that
  registers as a pool `frame_sink` member, drives the pure relay `Tracker` (§B),
  and performs its actions over real peers.

  Coverage (per §C / T4.1 contract):

    * `broadcast(txid, raw_bin)` announces `inv({:tx, txid})` to **exactly** the
      targets the injected `:selector` chose (selection goes through the seam,
      not `PeerRegistry.pids/1` map order);
    * a `getdata` for a pending tx serves the **exact binary** `raw_bin` back to
      the requesting peer (wire bytes, not ASCII hex);
    * a relay-back `inv` from any peer **not** in `announced_to` counts toward
      `bar` while a target echo never does; `bar` distinct non-targets →
      `:propagated` audit;
    * a peer `reject` is surfaced to the audit sink;
    * bounded resources — at `max_pending` the synchronous enqueue returns
      `{:error, :saturated}`; a repeat `getdata` from an already-served
      `(txid, peer)` is **not** re-served; an oversized `getdata` body is dropped
      by the shell's `Inv.parse`/`max_items` guard **before** the reducer;
    * the peer-churn race — an empty registry at enqueue time returns
      `{:error, :no_peers}` and announces nothing.

  Collaborators injected: real `:ready` `Peer`s over `Transport.Fake` (so emitted
  frames are genuinely captured via `Fake.sent/1`), a deterministic `:selector`,
  a stub `:audit` sink (messages to the test), and `now_fun`/timer intervals.
  No `Process.sleep`/`Process.alive?` — synchronize via `:sys.get_state` and
  `assert_receive`. `async: false` (singleton `PeerRegistry`).
  """
  use ExUnit.Case, async: false

  alias Athanor.P2P.Codec.VarInt
  alias Athanor.P2P.{Frame, Network, Peer, PeerRegistry, TxRelay}
  alias Athanor.P2P.Messages.{Inv, Reject, Version}
  alias Athanor.P2P.Transport.Fake

  @net Network.mainnet()

  # ── Helpers ──

  # Start a real peer and drive it to `:ready` over Transport.Fake so its
  # outbound frames are captured in `Fake.sent(socket)`.
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

  defp setup_registry, do: start_supervised!({PeerRegistry, name: PeerRegistry})

  defp register(pid, octet) do
    :ok = PeerRegistry.register(PeerRegistry, {{10, 0, octet, 1}, 8333}, pid)
    pid
  end

  defp start_relay(opts) do
    test = self()

    defaults = [
      audit: fn event -> send(test, {:audit, event}) end,
      now_fun: fn -> 0 end,
      tick_interval_ms: 60_000
    ]

    start_supervised!({TxRelay, Keyword.merge(defaults, opts)}, id: {TxRelay, make_ref()})
  end

  defp tx_with(byte) do
    tx = p2pkh_tx(:binary.copy(<<byte>>, 20))
    {BSV.Transaction.txid_binary(tx), BSV.Transaction.to_binary(tx)}
  end

  defp inv_frame(txid),
    do: %Frame{command: "inv", payload: Inv.serialize([{:tx, txid}])}

  defp getdata_frame(txid),
    do: %Frame{command: "getdata", payload: Inv.serialize([{:tx, txid}])}

  # ── Tests ──

  test "broadcast announces inv({:tx, txid}) to exactly the selector's targets" do
    setup_registry()
    {t, t_sock} = ready_peer()
    {x, x_sock} = ready_peer()
    {y, y_sock} = ready_peer()
    register(t, 1)
    register(x, 2)
    register(y, 3)

    # Deterministic selector: announce only to t, hold back x and y — regardless
    # of registry (map) order. This is what proves selection uses the seam.
    relay = start_relay(selector: fn _pids, _held -> {[t], [x, y]} end)

    {txid, raw} = tx_with(0x51)
    assert :ok = TxRelay.broadcast(relay, txid, raw)
    for p <- [t, x, y], do: _ = :sys.get_state(p)

    inv = Frame.encode(@net, :inv, Inv.serialize([{:tx, txid}]))
    assert inv in Fake.sent(t_sock)
    refute inv in Fake.sent(x_sock)
    refute inv in Fake.sent(y_sock)
  end

  test "getdata for a pending tx serves the exact binary raw bytes to that peer" do
    setup_registry()
    {t, t_sock} = ready_peer()
    register(t, 1)
    relay = start_relay(selector: fn _, _ -> {[t], []} end)

    {txid, raw} = tx_with(0x52)
    assert :ok = TxRelay.broadcast(relay, txid, raw)

    send(relay, {:peer, t, :frame, getdata_frame(txid)})
    _ = :sys.get_state(relay)
    _ = :sys.get_state(t)

    assert Frame.encode(@net, :tx, raw) in Fake.sent(t_sock)
  end

  test "propagated fires at bar distinct non-target relay-backs; a target echo never counts" do
    setup_registry()
    {t, _} = ready_peer()
    {x, _} = ready_peer()
    {y, _} = ready_peer()
    register(t, 1)
    register(x, 2)
    register(y, 3)
    # N == 3 → held = bar = 2; announce to t, hold back x and y.
    relay = start_relay(selector: fn _, _ -> {[t], [x, y]} end)

    {txid, raw} = tx_with(0x53)
    assert :ok = TxRelay.broadcast(relay, txid, raw)

    # Target echo — ignored.
    send(relay, {:peer, t, :frame, inv_frame(txid)})
    _ = :sys.get_state(relay)
    # First distinct non-target — not yet enough.
    send(relay, {:peer, x, :frame, inv_frame(txid)})
    _ = :sys.get_state(relay)
    refute_received {:audit, {:propagated, _}}
    # Second distinct non-target — crosses the bar.
    send(relay, {:peer, y, :frame, inv_frame(txid)})
    _ = :sys.get_state(relay)
    assert_receive {:audit, {:propagated, ^txid}}
  end

  test "a tx reject from a peer is surfaced to the audit sink" do
    setup_registry()
    {t, _} = ready_peer()
    register(t, 1)
    relay = start_relay(selector: fn _, _ -> {[t], []} end)

    {txid, raw} = tx_with(0x54)
    assert :ok = TxRelay.broadcast(relay, txid, raw)

    reject = %Reject{message: "tx", ccode: 0x10, reason: "bad-txns", data: txid}
    send(relay, {:peer, t, :frame, %Frame{command: "reject", payload: Reject.serialize(reject)}})
    _ = :sys.get_state(relay)

    assert_receive {:audit, {:rejected, ^txid, ^t, reason}}
    assert reason =~ "bad-txns"
  end

  test "getdata for a txid we are not broadcasting is ignored (no send_tx)" do
    setup_registry()
    {t, t_sock} = ready_peer()
    register(t, 1)
    relay = start_relay(selector: fn _, _ -> {[t], []} end)

    # Flush any handshake-tail send (getaddr) so the baseline is stable.
    _ = :sys.get_state(t)
    before = Fake.sent(t_sock)
    send(relay, {:peer, t, :frame, getdata_frame(:binary.copy(<<0x99>>, 32))})
    _ = :sys.get_state(relay)
    _ = :sys.get_state(t)

    assert Fake.sent(t_sock) == before
  end

  test "at max_pending a further broadcast returns {:error, :saturated}" do
    setup_registry()
    {t, _} = ready_peer()
    register(t, 1)
    relay = start_relay(selector: fn _, _ -> {[t], []} end, tracker: [max_pending: 1])

    {txid1, raw1} = tx_with(0x55)
    {txid2, raw2} = tx_with(0x56)
    assert :ok = TxRelay.broadcast(relay, txid1, raw1)
    assert {:error, :saturated} = TxRelay.broadcast(relay, txid2, raw2)
  end

  test "a repeat getdata from an already-served peer is not re-served" do
    setup_registry()
    {t, t_sock} = ready_peer()
    register(t, 1)
    relay = start_relay(selector: fn _, _ -> {[t], []} end)

    {txid, raw} = tx_with(0x57)
    assert :ok = TxRelay.broadcast(relay, txid, raw)

    gd = getdata_frame(txid)
    send(relay, {:peer, t, :frame, gd})
    _ = :sys.get_state(relay)
    _ = :sys.get_state(t)
    send(relay, {:peer, t, :frame, gd})
    _ = :sys.get_state(relay)
    _ = :sys.get_state(t)

    tx_frame = Frame.encode(@net, :tx, raw)
    assert Enum.count(Fake.sent(t_sock), &(&1 == tx_frame)) == 1
  end

  test "a getdata frame with an oversized inventory count is dropped before the reducer" do
    setup_registry()
    {t, t_sock} = ready_peer()
    register(t, 1)
    relay = start_relay(selector: fn _, _ -> {[t], []} end)

    {txid, raw} = tx_with(0x58)
    assert :ok = TxRelay.broadcast(relay, txid, raw)
    # Flush the announce `inv` cast to `t` before snapshotting, so the baseline
    # already includes it and the assert isolates the dropped getdata.
    _ = :sys.get_state(t)

    # A body declaring far more items than @max_inv_items: Inv.parse rejects the
    # count before reading any item, so the reducer is never called → no send_tx.
    oversized = %Frame{command: "getdata", payload: VarInt.write(1_000_000)}
    before = Fake.sent(t_sock)
    send(relay, {:peer, t, :frame, oversized})
    _ = :sys.get_state(relay)
    _ = :sys.get_state(t)

    assert Fake.sent(t_sock) == before
  end

  test "broadcast with an empty registry returns {:error, :no_peers} and announces nothing" do
    setup_registry()
    relay = start_relay([])

    {txid, raw} = tx_with(0x59)
    assert {:error, :no_peers} = TxRelay.broadcast(relay, txid, raw)
    refute_received {:audit, _}
  end
end
