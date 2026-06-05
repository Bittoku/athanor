defmodule Athanor.P2P.Peer.HandshakeTest do
  @moduledoc """
  Tests for the pure handshake reducer `Athanor.P2P.Peer.Handshake` (T1.2).

  This is the heart of Phase 1 and the highest-risk piece, so it is fully
  exercised offline with no process and no socket: `step(state, event)` is a
  deterministic reducer returning `{state, actions}`. We drive both frame
  orderings (version-first and verack-first) and every failure mode (reject,
  timeout, malformed version), and confirm mid-handshake ping/pong and the
  ignoring of unrelated frames.
  """
  use ExUnit.Case, async: true

  alias Athanor.P2P.{Frame, Network}
  alias Athanor.P2P.Messages.{Protoconf, Version}
  alias Athanor.P2P.Peer.Handshake

  setup do
    net = Network.mainnet()
    na = Version.net_addr(0, <<0::128>>, 0)

    our =
      %Version{
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
      peer: peer,
      version_frame: %Frame{command: "version", payload: Version.serialize(peer)},
      verack_frame: %Frame{command: "verack", payload: <<>>},
      # Expected outbound (encoded) frames the reducer should emit.
      our_version_out: Frame.encode(net, :version, Version.serialize(our)),
      verack_out: Frame.encode(net, :verack, <<>>),
      protoconf_out: Frame.encode(net, :protoconf, Protoconf.serialize(%Protoconf{}))
    }
  end

  defp new(ctx), do: Handshake.new(ctx.net, ctx.our)

  test "start emits our version frame and moves to :awaiting", ctx do
    {state, actions} = Handshake.step(new(ctx), :start)

    assert actions == [{:send, ctx.our_version_out}]
    assert state.status == :awaiting
  end

  test "receiving peer version replies verack + protoconf and records peer_version", ctx do
    {state, _} = Handshake.step(new(ctx), :start)
    {state, actions} = Handshake.step(state, {:frame, ctx.version_frame})

    assert actions == [{:send, ctx.verack_out}, {:send, ctx.protoconf_out}]
    assert state.got_peer_version?
    assert state.sent_our_verack?
    refute state.got_peer_verack?
    assert %Version{user_agent: "/Bitcoin SV:1.2.2/", start_height: 850_000} = state.peer_version
    # Not complete yet — peer's verack still outstanding.
    refute Enum.any?(actions, &match?({:done, _}, &1))
  end

  test "version then verack reaches :done with the peer version", ctx do
    {state, _} = Handshake.step(new(ctx), :start)
    {state, _} = Handshake.step(state, {:frame, ctx.version_frame})
    {state, actions} = Handshake.step(state, {:frame, ctx.verack_frame})

    assert state.got_peer_verack?
    assert state.status == :done
    assert [{:done, %Version{user_agent: "/Bitcoin SV:1.2.2/"}}] = actions
  end

  test "verack before version still reaches :done (order independence)", ctx do
    {s1, _} = Handshake.step(new(ctx), :start)

    # Reverse arrival order: verack first, then version.
    {s1, a_verack} = Handshake.step(s1, {:frame, ctx.verack_frame})
    assert a_verack == []
    refute s1.status == :done

    {s1, a_version} = Handshake.step(s1, {:frame, ctx.version_frame})

    assert s1.status == :done
    # The completing step emits the verack+protoconf sends AND the done.
    assert {:send, ctx.verack_out} in a_version
    assert {:send, ctx.protoconf_out} in a_version
    assert Enum.any?(a_version, &match?({:done, %Version{}}, &1))

    # Same terminal flags as the version-first ordering.
    assert s1.got_peer_version? and s1.got_peer_verack? and s1.sent_our_verack?
  end

  test "a ping mid-handshake is answered with a pong and does not complete", ctx do
    {state, _} = Handshake.step(new(ctx), :start)

    {state, actions} =
      Handshake.step(state, {:frame, %Frame{command: "ping", payload: <<42::little-64>>}})

    assert actions == [{:send, Frame.encode(ctx.net, :pong, <<42::little-64>>)}]
    refute state.status == :done
  end

  test "unrelated frames are ignored (no actions, no completion)", ctx do
    {state, _} = Handshake.step(new(ctx), :start)

    for cmd <- ["sendheaders", "addr", "feefilter"] do
      {next, actions} = Handshake.step(state, {:frame, %Frame{command: cmd, payload: <<>>}})
      assert actions == []
      refute next.status == :done
    end
  end

  test "a reject during handshake is a fatal error", ctx do
    {state, _} = Handshake.step(new(ctx), :start)
    {_state, actions} = Handshake.step(state, {:frame, %Frame{command: "reject", payload: <<>>}})

    assert actions == [{:error, :handshake_rejected}]
  end

  test "a timeout before completion is a fatal error", ctx do
    {state, _} = Handshake.step(new(ctx), :start)
    {_state, actions} = Handshake.step(state, :timeout)

    assert actions == [{:error, :handshake_timeout}]
  end

  test "a malformed version payload is a fatal :bad_version error", ctx do
    {state, _} = Handshake.step(new(ctx), :start)

    {_state, actions} =
      Handshake.step(state, {:frame, %Frame{command: "version", payload: <<1, 2, 3>>}})

    assert actions == [{:error, :bad_version}]
  end
end
