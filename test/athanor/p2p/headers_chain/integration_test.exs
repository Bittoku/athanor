defmodule Athanor.P2P.HeadersChain.IntegrationTest do
  @moduledoc """
  Real-socket end-to-end for the Phase 6 headers chain (T6.4): a fork delivered
  over loopback through the real `P2P.Supervisor` + a `FakePeerServer`, proving
  the wire path `socket → FrameBuffer → Peer → pool fan-out → HeadersChain → :on_tip`.

  The server is driven by `{:cmd, {:headers, body}}` so the test controls ordering
  precisely: it pushes a 2-block chain (extend), then a higher-work 3-block fork off
  the same root (reorg), and separately a persistently-detached run (a fork below
  the retained window) that escalates to `:reorg_too_deep`. The chain is seeded from
  an injected `:seed` and PoW is bypassed via the tree's `:pow_check` seam so the
  fixtures need no mining; tip events are captured through an injected `:on_tip`.

  `async: false` (real sockets + singleton `PeerRegistry` + SQL sandbox); a bounded
  `eventually/1` is the one allowed real-process reality check.
  """
  use Athanor.DataCase, async: false

  alias Athanor.P2P.Codec.{Hash, VarInt}
  alias Athanor.P2P.{FakePeerServer, HeadersChain, Network, PeerPool, PeerRegistry}
  alias Athanor.P2P.Messages.{BlockHeader, Version}
  alias Athanor.P2P.Transport.LoopbackRewrite

  @easy 0x207FFFFF
  @root :binary.copy(<<0x50>>, 32)
  @seed_height 100

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

  # A header off `prev_wire` (wire order) with a distinct nonce, easiest target.
  defp mk(prev_wire, nonce) do
    raw =
      <<1::little-32>> <>
        prev_wire <>
        <<0::256>> <> <<0::little-32>> <> <<@easy::little-32>> <> <<nonce::little-32>>

    %BlockHeader{raw: raw}
  end

  # `count` headers extending `start_wire`; `salt` makes sibling forks distinct.
  defp chain(start_wire, count, salt) do
    {hs, _} =
      Enum.map_reduce(1..count, start_wire, fn i, prev ->
        h = mk(prev, salt * 1_000 + i)
        {h, BlockHeader.hash(h)}
      end)

    hs
  end

  defp headers_body(headers) do
    VarInt.write(length(headers)) <> Enum.map_join(headers, &(&1.raw <> <<0>>))
  end

  defp display(headers), do: Enum.map(headers, &Hash.wire_to_display(BlockHeader.hash(&1)))

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

  # Boot the real P2P tree against a quiet FakePeerServer, seeding the header tree
  # from `@root`/`@seed_height` and reporting tip events to the test. Returns the
  # server pid (drive it with `{:cmd, {:headers, body}}`).
  defp start_p2p(headers_extra) do
    test = self()
    net = Network.testnet()

    {:ok, port, server} =
      FakePeerServer.start(
        network: net,
        report_to: self(),
        peer_version: ver(),
        announce_on_verack: false
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

    headers_opts =
      [
        seed: fn -> {:ok, @seed_height, @root} end,
        on_tip: fn ev -> send(test, {:tip, ev}) end,
        pow_check: fn _h, _b -> true end,
        tick_interval_ms: 60_000
      ] ++ headers_extra

    start_supervised!({Athanor.P2P.Supervisor, pool_config: config, headers_opts: headers_opts})
    eventually(fn -> length(PeerRegistry.pids(PeerRegistry)) == 1 end)
    server
  end

  test "a chain extension then a higher-work fork is reported as extend then reorg over real sockets" do
    server = start_p2p([])

    [a1, a2] = chain(@root, 2, 0)
    send(server, {:cmd, {:headers, headers_body([a1, a2])}})
    assert_receive {:tip, {:extend, extended}}, 4_000
    assert extended == display([a1, a2])

    # A 3-deep sibling branch off the same root out-works the 2-deep tip → reorg.
    [b1, b2, b3] = chain(@root, 3, 1)
    send(server, {:cmd, {:headers, headers_body([b1, b2, b3])}})

    assert_receive {:tip, {:reorg, %{orphan: orphan, connect: connect}}}, 4_000
    # Orphan set is tip→ancestor (the old branch, reversed); connect is ancestor→tip.
    assert orphan == display([a2, a1])
    assert connect == display([b1, b2, b3])
  end

  test "a persistently-detached run escalates to reorg_too_deep over real sockets" do
    server = start_p2p(max_detached_rounds: 2)

    # Solicit: advertise a block inv so our HeadersChain issues a getheaders. Only a
    # *solicited* detached reply counts toward escalation (note 963 B2).
    send(server, {:cmd, {:inv_block, :binary.copy(<<0xBB>>, 32)}})

    eventually(fn ->
      MapSet.size(:sys.get_state(Process.whereis(HeadersChain)).pending_getheaders) == 1
    end)

    # A header whose parent is neither the seed nor in the tree — a fork below the
    # retained window. Re-delivered, it stays detached each round; the re-request
    # after round 1 keeps the flow solicited.
    [orphan_header] = chain(:binary.copy(<<0x99>>, 32), 1, 7)
    body = headers_body([orphan_header])

    send(server, {:cmd, {:headers, body}})
    eventually(fn -> :sys.get_state(Process.whereis(HeadersChain)).detached_rounds == 1 end)

    send(server, {:cmd, {:headers, body}})
    assert_receive {:tip, {:reorg_too_deep, %{rounds: 2}}}, 4_000
  end
end
