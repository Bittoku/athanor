defmodule Athanor.P2P.PeerTest do
  @moduledoc """
  Tests for the `Athanor.P2P.Peer` GenServer connect + handshake path (T1.3),
  driven entirely over the deterministic `Transport.Fake` — no real sockets.

  The peer reports lifecycle to its `owner` via `{:peer, pid, :ready, version}`
  and `{:peer, pid, :down, reason}` messages, asserted with `assert_receive`.
  Process death is observed with `Process.monitor` + a `{:DOWN, ...}` assertion
  (never `Process.alive?`/sleep), per the project test rules.
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

    peer =
      %Version{
        addr_recv: na,
        addr_from: na,
        nonce: 2,
        user_agent: "/Bitcoin SV:1.2.2/",
        start_height: 850_000
      }

    %{
      net: net,
      our: our,
      our_version_out: Frame.encode(net, :version, Version.serialize(our)),
      verack_out: Frame.encode(net, :verack, <<>>),
      getaddr_out: Frame.encode(net, :getaddr, <<>>),
      peer_version_bytes: Frame.encode(net, :version, Version.serialize(peer)),
      verack_bytes: Frame.encode(net, :verack, <<>>)
    }
  end

  defp config(ctx, overrides \\ []) do
    base = %Peer.Config{
      host: ~c"127.0.0.1",
      port: 8333,
      network: ctx.net,
      our_version: ctx.our,
      transport: Fake,
      transport_opts: [fake: %{test: self()}],
      owner: self(),
      timeouts: %{handshake: 1_000}
    }

    struct!(base, overrides)
  end

  defp start_peer(config) do
    pid = start_supervised!({Peer, config}, restart: :temporary)
    assert_receive {:fake_handle, socket}
    {pid, socket}
  end

  test "on start, connects and sends our version frame first", ctx do
    {pid, socket} = start_peer(config(ctx))
    # Synchronize: ensure handle_continue(:connect) finished before inspecting sends.
    _ = :sys.get_state(pid)

    assert [first | _] = Fake.sent(socket)
    assert first == ctx.our_version_out
  end

  test "drives the handshake to :ready and sends verack, protoconf, then getaddr", ctx do
    {pid, socket} = start_peer(config(ctx))

    :ok = Fake.deliver(socket, ctx.peer_version_bytes)
    :ok = Fake.deliver(socket, ctx.verack_bytes)

    assert_receive {:peer, ^pid, :ready, %Version{user_agent: "/Bitcoin SV:1.2.2/"}}
    _ = :sys.get_state(pid)

    # version (on start) → verack + protoconf (on peer version) → getaddr (on ready).
    assert [version_out, verack_out, protoconf_bytes, getaddr_out] = Fake.sent(socket)
    assert version_out == ctx.our_version_out
    assert verack_out == ctx.verack_out
    assert getaddr_out == ctx.getaddr_out
    assert {:ok, %Frame{command: "protoconf"}, <<>>} = Frame.decode(ctx.net, protoconf_bytes)
  end

  test "a handshake that never completes times out, notifies down, and exits", ctx do
    {pid, _socket} = start_peer(config(ctx, timeouts: %{handshake: 50}))
    ref = Process.monitor(pid)

    assert_receive {:peer, ^pid, :down, :handshake_timeout}, 500
    assert_receive {:DOWN, ^ref, :process, ^pid, _reason}, 500
  end

  test "a refused connection notifies down and exits without a handle", ctx do
    config = config(ctx, transport_opts: [fake: %{test: self(), refuse: true}])
    pid = start_supervised!({Peer, config}, restart: :temporary)
    ref = Process.monitor(pid)

    assert_receive {:peer, ^pid, :down, {:connect, :econnrefused}}, 500
    assert_receive {:DOWN, ^ref, :process, ^pid, _reason}, 500
  end
end
