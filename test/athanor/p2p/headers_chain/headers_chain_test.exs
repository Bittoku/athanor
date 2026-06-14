defmodule Athanor.P2P.HeadersChainTest do
  @moduledoc """
  Tests for `Athanor.P2P.HeadersChain` (Phase 6 T6.2, §B) — the thin GenServer
  that seeds a synthetic root from REST, drives the pure `Tree` over `inv`/
  `headers`, issues `getheaders`, and surfaces tip events (extend/reorg) and the
  deep-reorg fallback to an injected `:on_tip` sink.

  Real `:ready` `Peer`s over `Transport.Fake` (so `getheaders` bytes are captured);
  injected `:seed`/`:on_tip`/`:selector`; PoW bypassed via the tree's `:pow_check`
  seam so header fixtures need no mining. `async: false` (singleton `PeerRegistry`).
  """
  use ExUnit.Case, async: false

  alias Athanor.P2P.Codec.Hash
  alias Athanor.P2P.{Frame, HeadersChain, Network, Peer, PeerRegistry}
  alias Athanor.P2P.Messages.{BlockHeader, Headers, Inv, Version}
  alias Athanor.P2P.Transport.Fake

  @net Network.mainnet()
  @easy 0x207FFFFF
  @version 70_016
  @seed :binary.copy(<<0x50>>, 32)

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

  defp mk(prev_wire, nonce) do
    raw =
      <<1::little-32>> <>
        prev_wire <>
        <<0::256>> <> <<0::little-32>> <> <<@easy::little-32>> <> <<nonce::little-32>>

    %BlockHeader{raw: raw}
  end

  defp chain(start_wire, count, salt \\ 0) do
    {hs, _} =
      Enum.map_reduce(1..count, start_wire, fn i, prev ->
        h = mk(prev, salt * 1_000 + i)
        {h, BlockHeader.hash(h)}
      end)

    hs
  end

  defp headers_frame(headers) do
    body =
      Athanor.P2P.Codec.VarInt.write(length(headers)) <>
        Enum.map_join(headers, &(&1.raw <> <<0>>))

    %Frame{command: "headers", payload: body}
  end

  defp inv_block(hash), do: %Frame{command: "inv", payload: Inv.serialize([{:block, hash}])}

  defp setup_registry, do: start_supervised!({PeerRegistry, name: PeerRegistry})

  defp register(pid, o),
    do: (:ok = PeerRegistry.register(PeerRegistry, {{10, 0, o, 1}, 8333}, pid)) && pid

  defp start_hc(opts) do
    test = self()

    defaults = [
      seed: fn -> {:ok, 100, @seed} end,
      on_tip: fn ev -> send(test, {:tip, ev}) end,
      registry: PeerRegistry,
      selector: fn pids -> List.first(pids) end,
      now_fun: fn -> 0 end,
      tick_interval_ms: 60_000,
      version: @version
    ]

    merged = Keyword.merge(defaults, opts)

    # Bypass PoW by default (fixtures are unmined); a test that passes its own
    # `:pow_check` or a `:pow_limit` exercises the real `Work.valid_pow?` gate.
    merged =
      if Keyword.has_key?(merged, :pow_check) or Keyword.has_key?(merged, :pow_limit),
        do: merged,
        else: Keyword.put(merged, :pow_check, fn _h, _b -> true end)

    # Bypass the F7.1 cw-144 DAA gate by default — these tests exercise the
    # GenServer plumbing (getheaders/reorg/escalation) with synthetic headers that
    # have no real difficulty window; the DAA gate is covered by its own tests.
    merged =
      if Keyword.has_key?(merged, :daa_check),
        do: merged,
        else: Keyword.put(merged, :daa_check, fn _p, _h, _a -> :ok end)

    start_supervised!({HeadersChain, merged}, id: {HeadersChain, make_ref()})
  end

  defp getheaders_bytes(locator),
    do:
      Frame.encode(
        @net,
        :getheaders,
        Headers.serialize_get_headers(@version, locator, <<0::256>>)
      )

  test "an inv(MSG_BLOCK) triggers getheaders with the seed locator to the advertising peer" do
    setup_registry()
    {peer, sock} = ready_peer()
    hc = start_hc([])

    send(hc, {:peer, peer, :frame, inv_block(:binary.copy(<<0xBB>>, 32))})
    _ = :sys.get_state(hc)
    _ = :sys.get_state(peer)

    # Fresh tree (only the synthetic root) → locator is just [seed].
    assert getheaders_bytes([@seed]) in Fake.sent(sock)
  end

  test "a headers run extends the tip and notifies :on_tip with display-order hashes" do
    setup_registry()
    {peer, _sock} = ready_peer()
    hc = start_hc([])

    [h1, h2] = chain(@seed, 2)
    send(hc, {:peer, peer, :frame, headers_frame([h1, h2])})

    assert_receive {:tip, {:extend, hashes}}

    assert hashes == [
             Hash.wire_to_display(BlockHeader.hash(h1)),
             Hash.wire_to_display(BlockHeader.hash(h2))
           ]
  end

  test "a higher-work fork notifies :on_tip with a {:reorg, …} of display-order sets" do
    setup_registry()
    {peer, _sock} = ready_peer()
    # max_detached_rounds high so detach doesn't interfere; both branches in-window.
    hc = start_hc([])

    [a1, a2] = chain(@seed, 2, 0)
    send(hc, {:peer, peer, :frame, headers_frame([a1, a2])})
    assert_receive {:tip, {:extend, _}}

    # A 3-deep sibling branch off the seed out-works the 2-deep tip → reorg.
    [b1, b2, b3] = chain(@seed, 3, 1)
    send(hc, {:peer, peer, :frame, headers_frame([b1, b2, b3])})

    assert_receive {:tip, {:reorg, %{orphan: orphan, connect: connect}}}

    assert orphan == [
             Hash.wire_to_display(BlockHeader.hash(a2)),
             Hash.wire_to_display(BlockHeader.hash(a1))
           ]

    assert connect ==
             Enum.map([b1, b2, b3], &Hash.wire_to_display(BlockHeader.hash(&1)))
  end

  # Solicit a getheaders to `peer` by advertising a block inv (the legitimate
  # chain-discovery flow), so its subsequent detached reply is counted.
  defp solicit(hc, peer) do
    send(hc, {:peer, peer, :frame, inv_block(:binary.copy(<<0xBB>>, 32))})
    _ = :sys.get_state(hc)
    :ok
  end

  test "persistent SOLICITED detached headers escalate to {:reorg_too_deep} after max_detached_rounds" do
    setup_registry()
    {peer, _sock} = ready_peer()
    register(peer, 1)
    hc = start_hc(max_detached_rounds: 2)

    # Our getheaders (triggered by the peer's block inv) makes the detached reply a
    # legitimate response; the re-request after each detached keeps the flow open.
    solicit(hc, peer)
    detached = chain(:binary.copy(<<0x99>>, 32), 1)

    send(hc, {:peer, peer, :frame, headers_frame(detached)})
    _ = :sys.get_state(hc)
    refute_received {:tip, {:reorg_too_deep, _}}

    send(hc, {:peer, peer, :frame, headers_frame(detached)})
    assert_receive {:tip, {:reorg_too_deep, _}}
  end

  test "an EMPTY headers response clears the solicitation token (note 979 B1)" do
    setup_registry()
    {peer, _sock} = ready_peer()
    register(peer, 1)
    hc = start_hc(max_detached_rounds: 2)

    # Solicit a getheaders, then answer it with an EMPTY headers vector (a valid
    # terminator). That must consume the outstanding request — it can't leave a
    # reusable "solicited" token for later junk.
    solicit(hc, peer)
    send(hc, {:peer, peer, :frame, headers_frame([])})
    _ = :sys.get_state(hc)

    # Subsequent unknown-parent headers are now UNSOLICITED and must not escalate.
    detached = chain(:binary.copy(<<0x99>>, 32), 1)
    send(hc, {:peer, peer, :frame, headers_frame(detached)})
    _ = :sys.get_state(hc)
    send(hc, {:peer, peer, :frame, headers_frame(detached)})
    _ = :sys.get_state(hc)

    refute_received {:tip, {:reorg_too_deep, _}}
  end

  test "UNSOLICITED unknown-parent headers from one peer do NOT escalate (note 963 B2)" do
    setup_registry()
    {peer, _sock} = ready_peer()
    register(peer, 1)
    hc = start_hc(max_detached_rounds: 2)

    # No getheaders was sent to this peer — these headers are unsolicited junk.
    # A single peer must not be able to force a global deep-reorg suspension.
    detached = chain(:binary.copy(<<0x99>>, 32), 1)

    send(hc, {:peer, peer, :frame, headers_frame(detached)})
    _ = :sys.get_state(hc)
    send(hc, {:peer, peer, :frame, headers_frame(detached)})
    _ = :sys.get_state(hc)

    refute_received {:tip, {:reorg_too_deep, _}}
  end

  test "solicited detached tracking is scoped per peer: one each from two peers does not escalate" do
    setup_registry()
    {p1, _s1} = ready_peer()
    {p2, _s2} = ready_peer()
    register(p1, 1)
    register(p2, 2)
    hc = start_hc(max_detached_rounds: 2)

    solicit(hc, p1)
    solicit(hc, p2)
    detached = chain(:binary.copy(<<0x99>>, 32), 1)

    send(hc, {:peer, p1, :frame, headers_frame(detached)})
    _ = :sys.get_state(hc)
    send(hc, {:peer, p2, :frame, headers_frame(detached)})
    _ = :sys.get_state(hc)

    refute_received {:tip, {:reorg_too_deep, _}}
  end

  test "a batch that advances the tip is never counted as a detached round (mixed batch)" do
    setup_registry()
    {peer, _sock} = ready_peer()
    register(peer, 1)
    hc = start_hc(max_detached_rounds: 2)

    solicit(hc, peer)

    # One batch that BOTH extends the tip (off the seed) and carries a detached
    # junk header (unknown parent). Progress must reset detached tracking.
    [e1, e2] = chain(@seed, 2)
    [junk] = chain(:binary.copy(<<0x99>>, 32), 1)
    send(hc, {:peer, peer, :frame, headers_frame([e1, e2, junk])})
    assert_receive {:tip, {:extend, _}}

    # Re-solicit and send the junk again — only one fresh round, so threshold 2 is
    # never reached because the progress reset cleared the count.
    solicit(hc, peer)
    send(hc, {:peer, peer, :frame, headers_frame([junk])})
    _ = :sys.get_state(hc)
    refute_received {:tip, {:reorg_too_deep, _}}
  end

  test "a malformed headers body is dropped (no tip event)" do
    setup_registry()
    {peer, _sock} = ready_peer()
    hc = start_hc([])

    # Declares 1 header but supplies no 80-byte body → Headers.parse :need_more/error.
    send(hc, {:peer, peer, :frame, %Frame{command: "headers", payload: <<1, 0, 0>>}})
    _ = :sys.get_state(hc)
    refute_received {:tip, _}
  end

  test "the default pow_check rejects an over-limit (easier-than-consensus) header" do
    setup_registry()
    {peer, _sock} = ready_peer()
    # No :pow_check override → the real Work.valid_pow? against the default
    # consensus pow-limit (0x1d00ffff). `mk/2` uses regtest-easy @easy bits, whose
    # target sits above the limit → the header must be rejected, not credited.
    hc = start_hc(pow_limit: 0x1D00FFFF)

    over_limit = mk(@seed, 1)
    send(hc, {:peer, peer, :frame, headers_frame([over_limit])})
    _ = :sys.get_state(hc)
    refute_received {:tip, _}
  end

  test "a failing seed does not crash the chain (fail-closed start)" do
    setup_registry()
    {peer, _sock} = ready_peer()
    hc = start_hc(seed: fn -> {:error, :node_down} end)

    # With no seed/tree, an inv is a harmless no-op (no crash, no getheaders need).
    ref = Process.monitor(hc)
    send(hc, {:peer, peer, :frame, inv_block(:binary.copy(<<0xBB>>, 32))})
    _ = :sys.get_state(hc)
    refute_received {:DOWN, ^ref, :process, ^hc, _}
  end
end
