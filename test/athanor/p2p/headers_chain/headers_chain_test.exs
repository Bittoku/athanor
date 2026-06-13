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
      version: @version,
      pow_check: fn _h, _b -> true end
    ]

    start_supervised!({HeadersChain, Keyword.merge(defaults, opts)},
      id: {HeadersChain, make_ref()}
    )
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

  test "persistent detached headers escalate to {:reorg_too_deep} after max_detached_rounds" do
    setup_registry()
    {peer, _sock} = ready_peer()
    register(peer, 1)
    hc = start_hc(max_detached_rounds: 2)

    # Headers whose parent is unknown (a below-window fork) → detached each round.
    detached = chain(:binary.copy(<<0x99>>, 32), 1)

    send(hc, {:peer, peer, :frame, headers_frame(detached)})
    _ = :sys.get_state(hc)
    refute_received {:tip, {:reorg_too_deep, _}}

    send(hc, {:peer, peer, :frame, headers_frame(detached)})
    assert_receive {:tip, {:reorg_too_deep, _}}
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
