defmodule Athanor.P2P.FakePeerServer do
  @moduledoc """
  A minimal in-test BSV node that genuinely speaks the wire protocol over a
  real `127.0.0.1` socket. Used by the Phase 1 loopback integration test (T1.7)
  to prove the `Transport.Gen` / active-mode / framing wiring works end to end,
  and reusable by the Phase 2 pool tests.

  On accept it runs a fixed script:

    1. reply to the client's `version` with its own `version` + `verack`;
    2. after the client's `verack`, send an `inv` (one tx hash) then a `ping`;
    3. when the client's answering `pong` arrives, report it to `:report_to`
       and close the connection (so the client observes `:down, :closed`).

  `start/1` returns `{:ok, port, server_pid}`; point a `Peer` at `port`.
  """

  alias Athanor.P2P.{Frame, FrameBuffer}
  alias Athanor.P2P.Messages.{Inv, Version}

  @doc """
  Starts the fake server on an ephemeral loopback port.

  Options: `:network` (required), `:report_to` (pid, required), `:peer_version`
  (the `%Version{}` to advertise, required), `:inv_hash` (32 bytes), and
  `:ping_nonce`.
  """
  @spec start(keyword()) :: {:ok, :inet.port_number(), pid()}
  def start(opts) do
    network = Keyword.fetch!(opts, :network)
    report_to = Keyword.fetch!(opts, :report_to)
    peer_version = Keyword.fetch!(opts, :peer_version)
    inv_hash = Keyword.get(opts, :inv_hash, :binary.copy(<<0xAB>>, 32))
    ping_nonce = Keyword.get(opts, :ping_nonce, 7777)

    {:ok, listen} =
      :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true, packet: :raw])

    {:ok, port} = :inet.port(listen)

    script = %{
      network: network,
      report_to: report_to,
      peer_version: peer_version,
      inv_hash: inv_hash,
      ping_nonce: ping_nonce
    }

    pid = spawn_link(fn -> accept(listen, script) end)
    {:ok, port, pid}
  end

  defp accept(listen, script) do
    {:ok, sock} = :gen_tcp.accept(listen)
    :gen_tcp.close(listen)
    serve(sock, FrameBuffer.new(script.network), %{hello?: false, payload?: false}, script)
  end

  defp serve(sock, buffer, progress, script) do
    case :gen_tcp.recv(sock, 0) do
      {:ok, data} ->
        {frames, buffer} = FrameBuffer.push(buffer, data)
        progress = Enum.reduce(frames, progress, &react(&1, sock, &2, script))
        serve(sock, buffer, progress, script)

      {:error, _closed} ->
        :ok
    end
  end

  # Respond to the client's version with our own version + verack (once).
  defp react(%Frame{command: "version"}, sock, %{hello?: false} = progress, script) do
    send_frame(sock, script.network, :version, Version.serialize(script.peer_version))
    send_frame(sock, script.network, :verack, <<>>)
    %{progress | hello?: true}
  end

  # After the client's verack, push an inv then a ping (once).
  defp react(%Frame{command: "verack"}, sock, %{payload?: false} = progress, script) do
    send_frame(sock, script.network, :inv, Inv.serialize([{:tx, script.inv_hash}]))
    send_frame(sock, script.network, :ping, <<script.ping_nonce::little-64>>)
    %{progress | payload?: true}
  end

  # The client answered our ping: report it and close the connection.
  defp react(%Frame{command: "pong", payload: <<nonce::little-64>>}, sock, progress, script) do
    send(script.report_to, {:server_received, :pong, nonce})
    :gen_tcp.close(sock)
    progress
  end

  # Anything else (protoconf, getaddr, duplicate version/verack) is ignored.
  defp react(%Frame{}, _sock, progress, _script), do: progress

  defp send_frame(sock, network, command, payload) do
    :ok = :gen_tcp.send(sock, Frame.encode(network, command, payload))
  end
end
