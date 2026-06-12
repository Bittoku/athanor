defmodule Athanor.P2P.PeerPool.IntegrationTest do
  @moduledoc """
  Real-socket integration test for the peer pool (T2.6): drives actual `Peer`s
  over `127.0.0.1` (through `Transport.LoopbackRewrite`) against several
  `FakePeerServer`s, started under the real `Athanor.P2P.Supervisor`.

  Diversity is kept ON: the address book sees distinct **/24** synthetic
  addresses (`1.0.n.1`) while the sockets all connect to loopback ports, proving
  the /24 invariant holds under real process churn. The test then kills a peer's
  server and asserts the pool self-heals back to `target`.

  `async: false` (real sockets + singleton registry). Uses a bounded
  `eventually/1` poll because handshakes complete asynchronously over real
  sockets — this is the one allowed real-process reality check.
  """
  use ExUnit.Case, async: false

  alias Athanor.P2P.{Network, PeerPool, PeerRegistry}
  alias Athanor.P2P.Messages.Version
  alias Athanor.P2P.Transport.LoopbackRewrite
  alias Athanor.P2P.FakePeerServer

  @count 3
  @target 2

  defp ver do
    na = Version.net_addr(0, <<0::128>>, 0)

    %Version{
      addr_recv: na,
      addr_from: na,
      nonce: 1,
      user_agent: "/Bitcoin SV:1.2.2/",
      start_height: 1_700_000
    }
  end

  defp syn(n), do: {{1, 0, n, 1}, 18_333}

  defp eventually(fun, timeout \\ 3_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_eventually(fun, deadline)
  end

  defp do_eventually(fun, deadline) do
    cond do
      fun.() ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("condition not met within timeout")

      true ->
        # Bounded wait between polls (real handshakes complete asynchronously).
        receive do
        after
          25 -> :ok
        end

        do_eventually(fun, deadline)
    end
  end

  setup do
    net = Network.testnet()

    # One lingering FakePeerServer per synthetic /24, on its own loopback port.
    servers =
      for n <- 1..@count, into: %{} do
        {:ok, port, pid} =
          FakePeerServer.start(
            network: net,
            report_to: self(),
            peer_version: ver(),
            linger: true
          )

        {n, %{port: port, pid: pid, syn: syn(n)}}
      end

    rewrite =
      for {_n, s} <- servers, into: %{} do
        {ip, _port} = s.syn
        {:inet.ntoa(ip), {~c"127.0.0.1", s.port}}
      end

    config = %PeerPool.Config{
      network: net,
      target: @target,
      our_version: ver(),
      transport: LoopbackRewrite,
      transport_opts: [rewrite: rewrite],
      resolver: fn _ -> {:error, :nxdomain} end,
      seeds: Enum.map(1..@count, &syn/1)
    }

    start_supervised!({Athanor.P2P.Supervisor, pool_config: config})
    %{servers: servers}
  end

  test "fills to target with distinct /24s over real sockets, then self-heals", %{
    servers: servers
  } do
    # The pool dials real Peers that handshake against the fake servers.
    eventually(fn -> length(PeerRegistry.addresses()) == @target end)

    live = PeerRegistry.addresses()
    assert length(live) == @target
    assert MapSet.size(PeerRegistry.slash24s()) == @target

    # Kill the server backing one live peer; its Peer sees the socket close.
    victim = hd(live)
    {_n, s} = Enum.find(servers, fn {_n, s} -> s.syn == victim end)
    Process.unlink(s.pid)
    Process.exit(s.pid, :kill)

    # The pool self-heals: the victim leaves and a fresh /24 (the spare) takes
    # its slot, back to target.
    eventually(fn ->
      addrs = PeerRegistry.addresses()
      length(addrs) == @target and victim not in addrs
    end)

    assert MapSet.size(PeerRegistry.slash24s()) == @target
  end
end
