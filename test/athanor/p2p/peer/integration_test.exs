defmodule Athanor.P2P.Peer.IntegrationTest do
  @moduledoc """
  End-to-end loopback test for `Athanor.P2P.Peer` over a real `127.0.0.1`
  socket with `Transport.Gen` (T1.7) — no Fake. This is the reality check that
  the active-mode message wiring, partial reads, and byte framing all work
  together against a genuine peer (`FakePeerServer`). If this fails where the
  Fake-driven tests pass, the bug is real-socket-only.
  """
  use ExUnit.Case, async: false

  alias Athanor.P2P.{FakePeerServer, Frame, Network}
  alias Athanor.P2P.Messages.{Inv, Version}
  alias Athanor.P2P.Peer
  alias Athanor.P2P.Transport

  test "handshakes, receives an inv, answers a ping, and observes the close" do
    net = Network.mainnet()
    na = Version.net_addr(0, <<0::128>>, 0)

    our = %Version{
      addr_recv: na,
      addr_from: na,
      nonce: 1,
      user_agent: "/Athanor:0.1.0/",
      start_height: 0
    }

    server_version =
      %Version{
        addr_recv: na,
        addr_from: na,
        nonce: 2,
        user_agent: "/Bitcoin SV:1.2.2/",
        start_height: 850_123
      }

    inv_hash = :binary.copy(<<0xAB>>, 32)

    {:ok, port, _server} =
      FakePeerServer.start(
        network: net,
        report_to: self(),
        peer_version: server_version,
        inv_hash: inv_hash,
        ping_nonce: 7777
      )

    config = %Peer.Config{
      host: ~c"127.0.0.1",
      port: port,
      network: net,
      our_version: our,
      transport: Transport.Gen,
      transport_opts: [],
      owner: self(),
      timeouts: %{handshake: 2_000}
    }

    pid = start_supervised!({Peer, config}, restart: :temporary)

    # Handshake completes against the real peer with a sane advertised version.
    assert_receive {:peer, ^pid, :ready,
                    %Version{user_agent: "/Bitcoin SV:1.2.2/", start_height: 850_123}},
                   2_000

    # The peer's inv is reassembled off the wire and forwarded with its hash.
    assert_receive {:peer, ^pid, :frame, %Frame{command: "inv", payload: inv_payload}}, 2_000
    assert {:ok, [{:tx, ^inv_hash}], <<>>} = Inv.parse(inv_payload)

    # Our automatic pong answered the server's ping (proven server-side).
    assert_receive {:server_received, :pong, 7777}, 2_000

    # When the server closes, the peer reports a clean :closed disconnect.
    assert_receive {:peer, ^pid, :down, :closed}, 2_000
  end
end
