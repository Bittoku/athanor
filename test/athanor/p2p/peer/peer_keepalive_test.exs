defmodule Athanor.P2P.PeerKeepaliveTest do
  @moduledoc """
  Tests for `Athanor.P2P.Peer` keepalive ping and inactivity timeout (T1.5).

  Timer behaviour is verified *deterministically* rather than by waiting on the
  wall clock: the real timers are armed with long intervals (so they never fire
  mid-test), and the test drives the logic by sending the timer messages
  directly to the peer. The inactivity timer is epoch-tagged, so a stale timer
  message (one from before an inbound reset) is provably ignored — this proves
  the reset without any `Process.sleep` (which the project test rules forbid).
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

    %{
      net: net,
      our: our,
      peer_version_bytes: Frame.encode(net, :version, Version.serialize(peer)),
      verack_bytes: Frame.encode(net, :verack, <<>>)
    }
  end

  # Long timer intervals so real timers never fire during the test; we inject
  # the timer messages ourselves to drive the logic deterministically.
  defp start_ready(ctx, timeouts) do
    config = %Peer.Config{
      host: ~c"127.0.0.1",
      port: 8333,
      network: ctx.net,
      our_version: ctx.our,
      transport: Fake,
      transport_opts: [fake: %{test: self()}],
      owner: self(),
      timeouts: Map.merge(%{handshake: 60_000}, timeouts)
    }

    pid = start_supervised!({Peer, config}, restart: :temporary)
    assert_receive {:fake_handle, socket}
    :ok = Fake.deliver(socket, ctx.peer_version_bytes)
    :ok = Fake.deliver(socket, ctx.verack_bytes)
    assert_receive {:peer, ^pid, :ready, _}
    {pid, socket}
  end

  test "the ping timer firing emits a ping and records the in-flight nonce", ctx do
    {pid, socket} = start_ready(ctx, %{ping_interval: 60_000})

    send(pid, :send_ping)
    nonce = :sys.get_state(pid).ping_nonce

    assert is_integer(nonce)
    assert Frame.encode(ctx.net, :ping, <<nonce::little-64>>) in Fake.sent(socket)
  end

  test "a matching pong clears the in-flight ping (no disconnect)", ctx do
    {pid, socket} = start_ready(ctx, %{ping_interval: 60_000})

    send(pid, :send_ping)
    nonce = :sys.get_state(pid).ping_nonce

    :ok = Fake.deliver(socket, Frame.encode(ctx.net, :pong, <<nonce::little-64>>))
    # A successful :sys.get_state proves the process is still alive.
    assert :sys.get_state(pid).ping_nonce == nil
  end

  test "the inactivity timer firing disconnects with :inactivity_timeout", ctx do
    {pid, _socket} = start_ready(ctx, %{inactivity: 60_000})
    ref = Process.monitor(pid)

    epoch = :sys.get_state(pid).inactivity_epoch
    send(pid, {:inactivity, epoch})

    assert_receive {:peer, ^pid, :down, :inactivity_timeout}, 500
    assert_receive {:DOWN, ^ref, :process, ^pid, _reason}, 500
  end

  test "an inbound frame resets the inactivity clock (stale timer ignored)", ctx do
    {pid, socket} = start_ready(ctx, %{inactivity: 60_000})
    stale_epoch = :sys.get_state(pid).inactivity_epoch

    # Inbound traffic resets the inactivity clock, bumping the epoch.
    :ok = Fake.deliver(socket, Frame.encode(ctx.net, :inv, <<0>>))
    fresh_epoch = :sys.get_state(pid).inactivity_epoch
    assert fresh_epoch != stale_epoch

    # The pre-reset timer message must be ignored — the peer stays alive.
    send(pid, {:inactivity, stale_epoch})
    refute_received {:peer, ^pid, :down, _}
    assert :sys.get_state(pid).phase == :ready

    # The current timer still disconnects.
    send(pid, {:inactivity, fresh_epoch})
    assert_receive {:peer, ^pid, :down, :inactivity_timeout}, 500
  end
end
