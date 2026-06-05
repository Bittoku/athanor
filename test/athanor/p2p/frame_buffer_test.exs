defmodule Athanor.P2P.FrameBufferTest do
  @moduledoc """
  Tests for `Athanor.P2P.FrameBuffer` (T1.1): the pure accumulator that
  reassembles arbitrary TCP byte chunks into whole `Frame`s.

  TCP gives no message boundaries — a single `recv` can carry half a frame,
  three frames, or a frame plus the start of the next. These tests pin every
  such boundary case, plus fatal decode-error propagation.
  """
  use ExUnit.Case, async: true

  alias Athanor.P2P.{Frame, FrameBuffer, Network}

  setup do
    net = Network.mainnet()
    verack = Frame.encode(net, :verack, <<>>)
    ping = Frame.encode(net, :ping, <<7::little-64>>)
    %{net: net, verack: verack, ping: ping}
  end

  test "a whole frame in one push yields one frame and an empty buffer", %{
    net: net,
    verack: verack
  } do
    {frames, buf} = FrameBuffer.push(FrameBuffer.new(net), verack)

    assert [%Frame{command: "verack", payload: <<>>}] = frames
    # A subsequent empty push produces nothing — the buffer held no remainder.
    assert {[], ^buf} = FrameBuffer.push(buf, <<>>)
  end

  test "a frame split across two pushes completes on the second", %{net: net, verack: verack} do
    <<head::binary-10, tail::binary>> = verack

    {[], buf} = FrameBuffer.push(FrameBuffer.new(net), head)
    {frames, _buf} = FrameBuffer.push(buf, tail)

    assert [%Frame{command: "verack"}] = frames
  end

  test "multiple frames in one push come out in order", %{net: net, verack: verack, ping: ping} do
    {frames, _buf} = FrameBuffer.push(FrameBuffer.new(net), verack <> ping <> verack)

    assert [
             %Frame{command: "verack"},
             %Frame{command: "ping", payload: <<7::little-64>>},
             %Frame{command: "verack"}
           ] = frames
  end

  test "a full frame plus a partial next frame retains the remainder", %{
    net: net,
    verack: verack,
    ping: ping
  } do
    <<partial::binary-10, _rest::binary>> = ping

    {frames, buf} = FrameBuffer.push(FrameBuffer.new(net), verack <> partial)
    assert [%Frame{command: "verack"}] = frames

    # Feeding the rest of the ping completes it — proving the 10 bytes were kept.
    <<_::binary-10, ping_rest::binary>> = ping
    {frames2, _buf} = FrameBuffer.push(buf, ping_rest)
    assert [%Frame{command: "ping", payload: <<7::little-64>>}] = frames2
  end

  test "a bad-magic chunk is a fatal error", %{net: net, verack: verack} do
    <<_magic::binary-4, rest::binary>> = verack
    bad = <<0, 0, 0, 0>> <> rest

    assert {:error, :bad_magic, _buf} = FrameBuffer.push(FrameBuffer.new(net), bad)
  end
end
