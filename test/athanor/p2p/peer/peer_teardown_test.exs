defmodule Athanor.P2P.PeerTeardownTest do
  @moduledoc """
  Tests for `Athanor.P2P.Peer` teardown and disconnect semantics (T1.6),
  driven over `Transport.Fake`.

  Disconnects are normal lifecycle, not crashes: each exit path notifies the
  owner with a clean reason and the process exits `:normal` (so a Phase 2
  supervisor's max-restarts is not tripped spuriously). Process death is
  observed with `Process.monitor` + `{:DOWN, ...}`, never `Process.alive?`.
  """
  use ExUnit.Case, async: true

  alias Athanor.P2P.{Frame, Network}
  alias Athanor.P2P.Messages.Version
  alias Athanor.P2P.Peer
  alias Athanor.P2P.Transport.Fake

  setup do
    net = Network.mainnet()
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
      timeouts: %{handshake: 60_000}
    }

    %{
      net: net,
      config: config,
      peer_version_bytes: Frame.encode(net, :version, Version.serialize(peer)),
      verack_bytes: Frame.encode(net, :verack, <<>>)
    }
  end

  defp start_ready(ctx) do
    pid = start_supervised!({Peer, ctx.config}, restart: :temporary)
    assert_receive {:fake_handle, socket}
    :ok = Fake.deliver(socket, ctx.peer_version_bytes)
    :ok = Fake.deliver(socket, ctx.verack_bytes)
    assert_receive {:peer, ^pid, :ready, _}
    {pid, socket}
  end

  test "a remote close notifies down :closed and exits cleanly", ctx do
    {pid, socket} = start_ready(ctx)
    ref = Process.monitor(pid)

    :ok = Fake.deliver_closed(socket)

    assert_receive {:peer, ^pid, :down, :closed}, 500
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 500
  end

  test "a socket error notifies down {:tcp_error, reason} and exits", ctx do
    {pid, socket} = start_ready(ctx)
    ref = Process.monitor(pid)

    :ok = Fake.deliver_error(socket, :econnreset)

    assert_receive {:peer, ^pid, :down, {:tcp_error, :econnreset}}, 500
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 500
  end

  test "a reject after :ready is forwarded to the owner and is not fatal", ctx do
    {pid, socket} = start_ready(ctx)

    :ok = Fake.deliver(socket, Frame.encode(ctx.net, :reject, <<>>))

    assert_receive {:peer, ^pid, :frame, %Frame{command: "reject"}}
    # Still alive and serving — a successful :sys.get_state proves it.
    assert :sys.get_state(pid).phase == :ready
    refute_received {:peer, ^pid, :down, _}
  end

  test "Peer.stop/1 gracefully stops with down :stopped", ctx do
    {pid, _socket} = start_ready(ctx)
    ref = Process.monitor(pid)

    :ok = Peer.stop(pid)

    assert_receive {:peer, ^pid, :down, :stopped}, 500
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 500
  end
end
