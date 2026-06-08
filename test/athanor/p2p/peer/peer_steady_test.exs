defmodule Athanor.P2P.PeerSteadyTest do
  @moduledoc """
  Tests for the `Athanor.P2P.Peer` steady-state receive loop after `:ready`
  (T1.4), driven over `Transport.Fake`.

  Post-handshake the peer forwards application frames to its owner, answers
  `ping` with `pong` locally (the owner is not bothered), reassembles multiple
  frames delivered in one chunk, treats a decode error as fatal, and re-arms
  `active: :once` after every chunk so a fast peer cannot flood the mailbox.
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
      timeouts: %{handshake: 1_000}
    }

    %{
      net: net,
      config: config,
      peer_version_bytes: Frame.encode(net, :version, Version.serialize(peer)),
      verack_bytes: Frame.encode(net, :verack, <<>>)
    }
  end

  # Start a peer and drive it through the handshake to :ready.
  defp start_ready(ctx) do
    pid = start_supervised!({Peer, ctx.config}, restart: :temporary)
    assert_receive {:fake_handle, socket}
    :ok = Fake.deliver(socket, ctx.peer_version_bytes)
    :ok = Fake.deliver(socket, ctx.verack_bytes)
    assert_receive {:peer, ^pid, :ready, _}
    {pid, socket}
  end

  test "forwards an inbound application frame to the owner", ctx do
    {pid, socket} = start_ready(ctx)
    inv = Frame.encode(ctx.net, :inv, <<1, 0::256>>)

    :ok = Fake.deliver(socket, inv)

    assert_receive {:peer, ^pid, :frame, %Frame{command: "inv", payload: <<1, 0::256>>}}
  end

  test "answers a ping with a pong locally and does not forward it", ctx do
    {pid, socket} = start_ready(ctx)

    :ok = Fake.deliver(socket, Frame.encode(ctx.net, :ping, <<99::little-64>>))
    _ = :sys.get_state(pid)

    assert Frame.encode(ctx.net, :pong, <<99::little-64>>) in Fake.sent(socket)
    refute_received {:peer, ^pid, :frame, _}
  end

  test "reassembles two frames delivered in one chunk, in order", ctx do
    {pid, socket} = start_ready(ctx)
    inv = Frame.encode(ctx.net, :inv, <<1, 0::256>>)
    headers = Frame.encode(ctx.net, :headers, <<0>>)

    :ok = Fake.deliver(socket, inv <> headers)

    assert_receive {:peer, ^pid, :frame, %Frame{command: "inv"}}
    assert_receive {:peer, ^pid, :frame, %Frame{command: "headers"}}
  end

  test "a bad-magic chunk is fatal: down :bad_magic and exit", ctx do
    {pid, socket} = start_ready(ctx)
    ref = Process.monitor(pid)

    :ok = Fake.deliver(socket, <<0, 0, 0, 0>> <> :binary.copy(<<0>>, 40))

    assert_receive {:peer, ^pid, :down, :bad_magic}, 500
    assert_receive {:DOWN, ^ref, :process, ^pid, _reason}, 500
  end

  test "re-arms active:once after each received chunk", ctx do
    {pid, socket} = start_ready(ctx)

    :ok = Fake.deliver(socket, Frame.encode(ctx.net, :inv, <<0>>))
    _ = :sys.get_state(pid)

    log = Fake.setopts_log(socket)
    assert log != []
    assert Enum.all?(log, &(&1 == [active: :once]))
  end
end
