defmodule Athanor.P2P.PeerSendFrameTest do
  @moduledoc """
  Tests the outbound-command seam `Peer.send_frame/3` (Phase 3, T3.S/§C): a
  public API that makes an already-handshaked `Peer` write an arbitrary frame
  (e.g. `getdata`) over its injected transport — the production path the
  mempool observer uses to request a tx. Ready-gated: a send before `:ready` is
  a no-op.
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

  test "send_frame after :ready writes the encoded frame", ctx do
    pid = start_supervised!({Peer, ctx.config}, restart: :temporary)
    assert_receive {:fake_handle, socket}
    :ok = Fake.deliver(socket, ctx.peer_version_bytes)
    :ok = Fake.deliver(socket, ctx.verack_bytes)
    assert_receive {:peer, ^pid, :ready, _}

    payload = <<1, 0::256>>
    :ok = Peer.send_frame(pid, :getdata, payload)
    _ = :sys.get_state(pid)

    assert Frame.encode(ctx.net, :getdata, payload) in Fake.sent(socket)
  end

  test "send_frame before :ready is a no-op", ctx do
    pid = start_supervised!({Peer, ctx.config}, restart: :temporary)
    assert_receive {:fake_handle, socket}
    # Still handshaking (no peer version/verack delivered).

    :ok = Peer.send_frame(pid, :getdata, <<9>>)
    _ = :sys.get_state(pid)

    refute Frame.encode(ctx.net, :getdata, <<9>>) in Fake.sent(socket)
  end
end
