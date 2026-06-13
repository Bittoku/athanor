defmodule Athanor.P2P.FakePeerServer do
  @moduledoc """
  A minimal in-test BSV node that genuinely speaks the wire protocol over a
  real `127.0.0.1` socket. Used by the Phase 1 loopback integration test (T1.7)
  to prove the `Transport.Gen` / active-mode / framing wiring works end to end,
  and reusable by the Phase 2 pool tests.

  On accept it runs a fixed script:

    1. reply to the client's `version` with its own `version` + `verack`;
    2. after the client's `verack`, send an `inv` (one tx hash) then a `ping`
       (unless `:announce_on_verack` is false — a quiet handshake);
    3. when the client's answering `pong` arrives, report it to `:report_to`
       and close the connection (so the client observes `:down, :closed`).

  `start/1` returns `{:ok, port, server_pid}`; point a `Peer` at `port`.

  ## Phase 4 (T4.3) — outbound broadcast round-trip

  For the self-broadcast integration test the server is driven, per-server, as
  either an **announce target** or a **held-back peer**:

    * `:announce_on_verack` (default `true`) — when `false`, the server stays
      quiet after the handshake (no `inv`/`ping`), so the only `inv` it sends is
      the explicit relay-back below.
    * `:serve_on_inv` (default `false`) — when `true`, an `inv` *from the client*
      (our announce) is answered with a `getdata` for those tx hashes; the
      configured `:tx_payload` is then served back, so the **announce target**
      pulls our tx over the wire. A received `tx` is reported as
      `{:server_received, :tx, byte_size}`.
    * `:relay_back_hash` (32 bytes) + the `{:cmd, :relay_back}` message — on that
      message the server sends `inv(relay_back_hash)`, simulating a peer that
      learned the tx **from the network** (a relay-back). The test sends the
      command *after* `broadcast_tx` has recorded the pending broadcast, so the
      relay-back is counted rather than dropped as unknown.

  ## Phase 6 (T6.4) — headers over the wire

  For the fork-over-sockets headers integration the server serves headers
  on demand:

    * the `{:cmd, {:headers, payload}}` message — the server sends a `headers`
      frame carrying the given pre-serialized body, so a test can push a chain
      extension, a higher-work fork, or a detached run over the real socket and
      observe the `HeadersChain`'s tip events. A client `getheaders` is otherwise
      ignored (the test drives ordering explicitly via this command).
    * the `{:cmd, {:inv_block, hash}}` message — the server advertises a block
      inventory, so the client's `HeadersChain` issues a `getheaders` and thereby
      *solicits* the subsequent `headers` reply (needed to exercise the solicited
      detached-escalation path).
  """

  alias Athanor.P2P.{Frame, FrameBuffer}
  alias Athanor.P2P.Messages.{Inv, Version}

  @max_inv_items 50_000

  @doc """
  Starts the fake server on an ephemeral loopback port.

  Options: `:network` (required), `:report_to` (pid, required), `:peer_version`
  (the `%Version{}` to advertise, required), `:inv_hash` (32 bytes),
  `:ping_nonce`, `:linger`, `:tx_payload`, and the Phase-4 round-trip seams
  `:announce_on_verack`, `:serve_on_inv`, `:relay_back_hash` (see the module doc).
  """
  @spec start(keyword()) :: {:ok, :inet.port_number(), pid()}
  def start(opts) do
    network = Keyword.fetch!(opts, :network)
    report_to = Keyword.fetch!(opts, :report_to)
    peer_version = Keyword.fetch!(opts, :peer_version)
    inv_hash = Keyword.get(opts, :inv_hash, :binary.copy(<<0xAB>>, 32))
    ping_nonce = Keyword.get(opts, :ping_nonce, 7777)
    # When true, keep the connection open after pong (a persistent peer, used by
    # the pool integration test); when false (default), close after pong to
    # exercise the client's :closed path (Phase 1 T1.7).
    linger = Keyword.get(opts, :linger, false)
    # When set, a `getdata` from the client is answered with this raw `tx`
    # payload (Phase 3 T3.4: prove the observer's inv→getdata→tx exchange). When
    # nil (default), `getdata` is ignored, leaving every prior test unchanged.
    tx_payload = Keyword.get(opts, :tx_payload)

    {:ok, listen} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true, packet: :raw])

    {:ok, port} = :inet.port(listen)

    script = %{
      network: network,
      report_to: report_to,
      peer_version: peer_version,
      inv_hash: inv_hash,
      ping_nonce: ping_nonce,
      linger: linger,
      tx_payload: tx_payload,
      # Phase 4 round-trip seams (default to the pre-Phase-4 behavior).
      announce_on_verack: Keyword.get(opts, :announce_on_verack, true),
      serve_on_inv: Keyword.get(opts, :serve_on_inv, false),
      relay_back_hash: Keyword.get(opts, :relay_back_hash),
      # Phase 5 pull-fetch: serve `tx_payload` only for this txid, else notfound.
      serve_txid: Keyword.get(opts, :serve_txid)
    }

    pid = spawn_link(fn -> accept(listen, script) end)
    {:ok, port, pid}
  end

  defp accept(listen, script) do
    {:ok, sock} = :gen_tcp.accept(listen)
    :gen_tcp.close(listen)
    serve(sock, FrameBuffer.new(script.network), %{hello?: false, payload?: false}, script)
  end

  # A blocking recv with a short timeout so the process stays responsive to
  # out-of-band test commands (e.g. `{:cmd, :relay_back}`) between socket reads.
  defp serve(sock, buffer, progress, script) do
    progress = drain_commands(sock, progress, script)

    case :gen_tcp.recv(sock, 0, 50) do
      {:ok, data} ->
        {frames, buffer} = FrameBuffer.push(buffer, data)
        progress = Enum.reduce(frames, progress, &react(&1, sock, &2, script))
        serve(sock, buffer, progress, script)

      {:error, :timeout} ->
        serve(sock, buffer, progress, script)

      {:error, _closed} ->
        :ok
    end
  end

  # Drain queued test commands without blocking. `{:cmd, :relay_back}` sends an
  # `inv(relay_back_hash)` — a peer advertising the tx back to us (T4.3).
  defp drain_commands(sock, progress, script) do
    receive do
      {:cmd, :relay_back} ->
        if is_binary(script.relay_back_hash),
          do:
            send_frame(sock, script.network, :inv, Inv.serialize([{:tx, script.relay_back_hash}]))

        drain_commands(sock, progress, script)

      # Phase 6 (T6.4): advertise a block inventory so the client's HeadersChain
      # issues a `getheaders` — i.e. it *solicits* our subsequent `headers` reply.
      {:cmd, {:inv_block, hash}} when is_binary(hash) ->
        send_frame(sock, script.network, :inv, Inv.serialize([{:block, hash}]))
        drain_commands(sock, progress, script)

      # Phase 6 (T6.4): push a pre-serialized `headers` body over the wire so the
      # client's HeadersChain folds it (extend / fork / detached run).
      {:cmd, {:headers, payload}} when is_binary(payload) ->
        send_frame(sock, script.network, :headers, payload)
        drain_commands(sock, progress, script)
    after
      0 -> progress
    end
  end

  # Respond to the client's version with our own version + verack (once).
  defp react(%Frame{command: "version"}, sock, %{hello?: false} = progress, script) do
    send_frame(sock, script.network, :version, Version.serialize(script.peer_version))
    send_frame(sock, script.network, :verack, <<>>)
    %{progress | hello?: true}
  end

  # After the client's verack, push an inv then a ping (once) — unless this
  # server is configured for a quiet handshake (Phase 4 T4.3).
  defp react(%Frame{command: "verack"}, sock, %{payload?: false} = progress, script) do
    if script.announce_on_verack do
      send_frame(sock, script.network, :inv, Inv.serialize([{:tx, script.inv_hash}]))
      send_frame(sock, script.network, :ping, <<script.ping_nonce::little-64>>)
    end

    %{progress | payload?: true}
  end

  # The client answered our ping: report it and (unless lingering) close.
  defp react(%Frame{command: "pong", payload: <<nonce::little-64>>}, sock, progress, script) do
    send(script.report_to, {:server_received, :pong, nonce})
    unless script.linger, do: :gen_tcp.close(sock)
    progress
  end

  # The client announced a tx to us (our broadcast): request it so the announce
  # target pulls our tx over the wire (T4.3, `serve_on_inv`).
  defp react(
         %Frame{command: "inv", payload: payload},
         sock,
         progress,
         %{serve_on_inv: true} = script
       ) do
    case Inv.parse(payload, max_items: @max_inv_items) do
      {:ok, items, _rest} ->
        for {:tx, hash} <- items,
            do: send_frame(sock, script.network, :getdata, Inv.serialize([{:tx, hash}]))

      _ ->
        :ok
    end

    progress
  end

  # Answer a `getdata` with the configured raw tx (T3.4/T4.3/T5.5). The peer
  # requests the tx it was inv'd (or pull-fetched); we serve the bytes whose hash
  # the client will verify. When `:serve_txid` is set (T5.5 pull-fetch), only that
  # txid is served — any other requested txid gets a `notfound` (so the fetcher
  # falls back to REST). With `:serve_txid` nil (default) we serve for any getdata,
  # leaving every prior test unchanged.
  defp react(
         %Frame{command: "getdata", payload: body},
         sock,
         progress,
         %{tx_payload: payload} = script
       )
       when is_binary(payload) do
    requested =
      case Inv.parse(body, max_items: @max_inv_items) do
        {:ok, items, _rest} -> for {:tx, hash} <- items, do: hash
        _ -> []
      end

    # Report the requested hashes (wire order, as on the wire) so a test can
    # assert the client issued the getdata for the expected txid (T5.5 boundary).
    send(script.report_to, {:server_received, :getdata, requested})

    cond do
      is_nil(script.serve_txid) or script.serve_txid in requested ->
        send_frame(sock, script.network, :tx, payload)

      requested != [] ->
        send_frame(
          sock,
          script.network,
          :notfound,
          Inv.serialize(Enum.map(requested, &{:tx, &1}))
        )

      true ->
        :ok
    end

    progress
  end

  # The client served us a tx (T4.3 round-trip evidence): report it.
  defp react(%Frame{command: "tx", payload: payload}, _sock, progress, script) do
    send(script.report_to, {:server_received, :tx, byte_size(payload)})
    progress
  end

  # Anything else (protoconf, getaddr, duplicate version/verack, or a getdata
  # with no tx configured) is ignored.
  defp react(%Frame{}, _sock, progress, _script), do: progress

  defp send_frame(sock, network, command, payload) do
    :ok = :gen_tcp.send(sock, Frame.encode(network, command, payload))
  end
end
