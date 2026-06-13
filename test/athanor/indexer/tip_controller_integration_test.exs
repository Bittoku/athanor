defmodule Athanor.Indexer.TipControllerIntegrationTest do
  @moduledoc """
  Phase 7 F7.2 (T7.4) — real-socket end-to-end for the advisory-until-RPC-confirmed
  model: a P2P headers fork delivered over loopback through the real `P2P.Supervisor`
  + `FakePeerServer` reaches the **real** `TipController` (wired as `HeadersChain`'s
  `:on_tip`), which runs an **RPC-confirmed reconcile** against an injected node
  "world" and applies — keeping the index **contiguous** (I1) across catch-up and
  reorg. A ZMQ-style hint goes through the same controller (it cannot bypass it).

  The P2P headers are advisory: their content only *triggers* a reconcile; the
  blocks actually applied come from the RPC node (the injected canonical chain), so
  the P2P fork and the node are deliberately decoupled. `async: false` (real
  sockets + singleton registry + SQL sandbox).
  """
  use Athanor.DataCase, async: false

  alias Athanor.Indexer.TipController
  alias Athanor.P2P.Codec.VarInt
  alias Athanor.P2P.{FakePeerServer, Network, PeerPool, PeerRegistry}
  alias Athanor.P2P.Messages.{BlockHeader, Version}
  alias Athanor.P2P.Transport.LoopbackRewrite

  @easy 0x207FFFFF
  @root :binary.copy(<<0x50>>, 32)

  defp ver do
    na = Version.net_addr(0, <<0::128>>, 0)
    %Version{addr_recv: na, addr_from: na, nonce: 1, user_agent: "/BSV/", start_height: 1}
  end

  defp mk(prev_wire, nonce) do
    raw =
      <<1::little-32>> <>
        prev_wire <>
        <<0::256>> <> <<0::little-32>> <> <<@easy::little-32>> <> <<nonce::little-32>>

    %BlockHeader{raw: raw}
  end

  defp chain(start_wire, count, salt) do
    {hs, _} =
      Enum.map_reduce(1..count, start_wire, fn i, prev ->
        h = mk(prev, salt * 1_000 + i)
        {h, BlockHeader.hash(h)}
      end)

    hs
  end

  defp headers_body(headers),
    do: VarInt.write(length(headers)) <> Enum.map_join(headers, &(&1.raw <> <<0>>))

  defp eventually(fun, timeout \\ 4_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_eventually(fun, deadline)
  end

  defp do_eventually(fun, deadline) do
    cond do
      res = fun.() ->
        res

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("not met in time")

      true ->
        receive do
        after
          25 -> :ok
        end && do_eventually(fun, deadline)
    end
  end

  defp drain(pid, n \\ 40)
  defp drain(pid, 0), do: :sys.get_state(pid)

  defp drain(pid, n) do
    s = :sys.get_state(pid)
    if s.machine.in_flight or s.machine.pending, do: drain(pid, n - 1), else: s
  end

  # The RPC node "world" + the local index, as one Agent. `canonical` is the node's
  # block hash per height; `node_tip` the node tip; `local`/`local_chain` the index.
  defp node_hex(height, fork \\ 0), do: Base.encode16(<<fork::8, height::16>>, case: :lower)

  defp start_world do
    {:ok, agent} =
      Agent.start_link(fn ->
        canonical = for h <- 100..105, into: %{}, do: {h, node_hex(h)}
        %{canonical: canonical, node_tip: 105, local: 105, local_chain: canonical}
      end)

    agent
  end

  defp start_controller(agent) do
    start_supervised!({
      TipController,
      # The Agent world starts already synced (local 105), so this test exercises
      # reconcile/apply over sockets, not bootstrap capture — start bootstrapped.
      name: TipController,
      bootstrapped: true,
      rpc_height: fn -> {:ok, Agent.get(agent, & &1.node_tip)} end,
      rpc_hash_at: fn h -> Agent.get(agent, & &1.canonical[h]) end,
      local_height: fn -> Agent.get(agent, & &1.local) end,
      local_hash_at: fn h -> Agent.get(agent, & &1.local_chain[h]) end,
      apply_fun: fn _proc, %{rollback_to: rb, connect: connect} ->
        apply_to_world(agent, rb, connect)
      end,
      batch: 10,
      tick_interval_ms: 60_000
    })
  end

  # Mimic BlockProcessor.apply_branch against the world: roll back the local chain to
  # the fork, then append the contiguous canonical blocks (their heights are known
  # from the start: rb+1 for a reorg, local+1 for a catch-up).
  defp apply_to_world(agent, rollback_to, connect) do
    Agent.get_and_update(agent, fn w ->
      w = if is_integer(rollback_to), do: rollback(w, rollback_to), else: w
      start = (rollback_to || w.local) + 1
      heights = if connect == [], do: [], else: Enum.to_list(start..(start + length(connect) - 1))

      local_chain =
        Enum.reduce(heights, w.local_chain, fn h, acc -> Map.put(acc, h, w.canonical[h]) end)

      new_local = List.last(heights) || w.local
      {{:ok, new_local}, %{w | local: new_local, local_chain: local_chain}}
    end)
  end

  defp rollback(w, height) do
    chain = for {h, v} <- w.local_chain, h <= height, into: %{}, do: {h, v}
    %{w | local: height, local_chain: chain}
  end

  # The index is contiguous (I1) and matches the node up to its tip.
  defp assert_contiguous!(agent) do
    w = Agent.get(agent, & &1)
    heights = w.local_chain |> Map.keys() |> Enum.sort()
    assert heights == Enum.to_list(100..w.local), "index must be contiguous 100..#{w.local}"

    for {h, v} <- w.local_chain,
        do: assert(v == w.canonical[h], "index must match the node at #{h}")
  end

  defp start_p2p do
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

    # on_tip is wired exactly as production (&TipController.notify_tip/1) so a P2P
    # tip event becomes a hint to the real controller; seed/pow are test fixtures.
    headers_opts = [
      seed: fn -> {:ok, 105, @root} end,
      on_tip: &TipController.notify_tip/1,
      pow_check: fn _h, _b -> true end,
      tick_interval_ms: 60_000
    ]

    start_supervised!({Athanor.P2P.Supervisor, pool_config: config, headers_opts: headers_opts})
    eventually(fn -> length(PeerRegistry.pids(PeerRegistry)) == 1 end)
    server
  end

  test "a P2P extend over sockets drives an RPC-confirmed catch-up; the index stays contiguous" do
    agent = start_world()
    tc = start_controller(agent)
    server = start_p2p()

    # The node advances to 110; a P2P extend (any valid headers run) hints the controller.
    Agent.update(agent, fn w ->
      canonical = Enum.reduce(106..110, w.canonical, &Map.put(&2, &1, node_hex(&1)))
      %{w | canonical: canonical, node_tip: 110}
    end)

    [h1, h2] = chain(@root, 2, 0)
    send(server, {:cmd, {:headers, headers_body([h1, h2])}})

    eventually(fn -> Agent.get(agent, & &1.local) == 110 end)
    _ = drain(tc)
    assert_contiguous!(agent)
  end

  test "a P2P reorg over sockets drives an RPC-confirmed rollback + reapply; still contiguous" do
    agent = start_world()
    tc = start_controller(agent)
    server = start_p2p()

    # The node reorgs: heights 103..107 are replaced by a different branch (fork 1),
    # tip now 107. The local index (at 105 on fork 0) diverges above 102.
    Agent.update(agent, fn w ->
      canonical =
        Enum.reduce(103..107, w.canonical, fn h, acc -> Map.put(acc, h, node_hex(h, 1)) end)

      %{w | canonical: canonical, node_tip: 107}
    end)

    [b1, b2, b3] = chain(@root, 3, 1)
    send(server, {:cmd, {:headers, headers_body([b1, b2, b3])}})

    eventually(fn -> Agent.get(agent, & &1.local) == 107 end)
    _ = drain(tc)
    # Index rolled back to the common ancestor (102) and re-applied the canonical
    # branch; it now matches the node tip and is contiguous.
    assert_contiguous!(agent)
    assert Agent.get(agent, & &1.canonical[105]) == node_hex(105, 1)
    assert Agent.get(agent, & &1.local_chain[105]) == node_hex(105, 1)
  end

  test "a ZMQ-style hint goes through the controller (cannot bypass it)" do
    agent = start_world()
    tc = start_controller(agent)
    _server = start_p2p()

    Agent.update(agent, fn w ->
      %{w | canonical: Map.put(w.canonical, 106, node_hex(106)), node_tip: 106}
    end)

    # A ZMQ hashblock hint — routed through TipController, not a direct mutation.
    TipController.hint(:zmq, <<0xAB>>)
    eventually(fn -> Agent.get(agent, & &1.local) == 106 end)
    _ = drain(tc)
    assert_contiguous!(agent)
  end
end
