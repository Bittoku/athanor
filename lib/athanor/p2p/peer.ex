defmodule Athanor.P2P.Peer do
  @moduledoc """
  A single outbound BSV peer connection (Phase 1, T1.3+).

  A thin GenServer shell around the pure protocol core (`Peer.Handshake` +
  `FrameBuffer`): it performs IO through an injected `Transport`, arms timers,
  and reports lifecycle to its `owner`. All handshake/dispatch decisions live in
  the pure modules, so this process stays small and the logic stays testable.

  ## Lifecycle messages (to `config.owner`)

    * `{:peer, pid, :ready, peer_version}` — handshake complete.
    * `{:peer, pid, :frame, %Frame{}}` — an inbound frame after `:ready`
      (steady state; added in T1.4).
    * `{:peer, pid, :down, reason}` — the connection ended (timeout, reject,
      connect failure, close, ...). The process then exits cleanly so a
      supervisor can decide whether to restart.

  Disconnects are a normal part of the lifecycle, so the process exits
  `:normal`; the *reason* is conveyed in the `:down` message, not the exit
  reason. The child spec is `restart: :temporary` (Phase 2's pool supervises
  peers with its own backoff policy).
  """

  use GenServer

  alias Athanor.P2P.{Frame, FrameBuffer}
  alias Athanor.P2P.Peer.{Config, Handshake}

  @default_connect_timeout 5_000
  @default_handshake_timeout 30_000

  @doc false
  def child_spec(%Config{} = config) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, [config]}, restart: :temporary}
  end

  @doc "Starts a peer for `config`. Connects and handshakes asynchronously."
  @spec start_link(Config.t()) :: GenServer.on_start()
  def start_link(%Config{} = config), do: GenServer.start_link(__MODULE__, config)

  @impl true
  def init(%Config{} = config) do
    state = %{
      config: config,
      socket: nil,
      phase: :connecting,
      handshake: nil,
      buffer: nil,
      hs_timer: nil,
      peer_version: nil
    }

    {:ok, state, {:continue, :connect}}
  end

  @impl true
  def handle_continue(:connect, %{config: c} = state) do
    case c.transport.connect(c.host, c.port, c.transport_opts, connect_timeout(c)) do
      {:ok, socket} ->
        handshake = Handshake.new(c.network, c.our_version)
        buffer = FrameBuffer.new(c.network)
        {handshake, actions} = Handshake.step(handshake, :start)
        timer = Process.send_after(self(), :handshake_timeout, handshake_timeout(c))

        state = %{
          state
          | socket: socket,
            handshake: handshake,
            buffer: buffer,
            hs_timer: timer,
            phase: :handshaking
        }

        case apply_actions(actions, state) do
          {:ok, state} -> {:noreply, state}
          {:stop, reason, state} -> {:stop, reason, state}
        end

      {:error, reason} ->
        notify(state, {:down, {:connect, reason}})
        {:stop, :normal, state}
    end
  end

  @impl true
  def handle_info({:tcp, socket, data}, %{socket: socket} = state) do
    case FrameBuffer.push(state.buffer, data) do
      {frames, buffer} ->
        # Re-arm `active: :once` after a successfully-processed chunk so a fast
        # peer cannot flood the mailbox; on a fatal error we are exiting anyway.
        case dispatch(frames, %{state | buffer: buffer}) do
          {:noreply, state} -> {:noreply, rearm(state)}
          {:stop, reason, state} -> {:stop, reason, state}
        end

      {:error, reason, buffer} ->
        notify(state, {:down, reason})
        {:stop, :normal, %{state | buffer: buffer}}
    end
  end

  def handle_info(:handshake_timeout, %{phase: :handshaking} = state) do
    {handshake, actions} = Handshake.step(state.handshake, :timeout)

    case apply_actions(actions, %{state | handshake: handshake}) do
      {:ok, state} -> {:noreply, state}
      {:stop, reason, state} -> {:stop, reason, state}
    end
  end

  # A handshake timer that fires after the handshake already completed is moot.
  def handle_info(:handshake_timeout, state), do: {:noreply, state}

  ## Frame dispatch

  # During the handshake, frames drive the reducer. Once it reaches :ready, any
  # remaining frames in the same chunk are handled as steady state (T1.4).
  defp dispatch(frames, %{phase: :handshaking} = state), do: run_handshake(frames, state)
  defp dispatch(frames, %{phase: :ready} = state), do: run_steady(frames, state)

  defp run_handshake([], state), do: {:noreply, state}

  defp run_handshake([frame | rest], state) do
    {handshake, actions} = Handshake.step(state.handshake, {:frame, frame})

    case apply_actions(actions, %{state | handshake: handshake}) do
      {:ok, %{phase: :ready} = state} -> run_steady(rest, state)
      {:ok, state} -> run_handshake(rest, state)
      {:stop, reason, state} -> {:stop, reason, state}
    end
  end

  # Steady state: handle protocol housekeeping locally, forward everything else
  # to the owner. Keepalive `ping`s are answered with `pong` without bothering
  # the owner; the owner decides what to do with application frames.
  defp run_steady([], state), do: {:noreply, state}

  defp run_steady([frame | rest], state) do
    {:ok, state} = handle_steady_frame(frame, state)
    run_steady(rest, state)
  end

  defp handle_steady_frame(
         %Frame{command: "ping", payload: <<nonce::little-64>>},
         %{config: c, socket: socket} = state
       ) do
    c.transport.send(socket, Frame.encode(c.network, :pong, <<nonce::little-64>>))
    {:ok, state}
  end

  defp handle_steady_frame(%Frame{} = frame, state) do
    notify(state, {:frame, frame})
    {:ok, state}
  end

  ## Reducer action interpreter

  defp apply_actions([], state), do: {:ok, state}

  defp apply_actions([action | rest], state) do
    case apply_action(action, state) do
      {:ok, state} -> apply_actions(rest, state)
      {:stop, reason, state} -> {:stop, reason, state}
    end
  end

  defp apply_action({:send, bytes}, %{config: c, socket: socket} = state) do
    c.transport.send(socket, bytes)
    {:ok, state}
  end

  defp apply_action({:done, peer_version}, state), do: {:ok, become_ready(state, peer_version)}

  defp apply_action({:error, reason}, state) do
    notify(state, {:down, reason})
    {:stop, :normal, state}
  end

  # Transition to steady state: stop the handshake timer, tell the owner, and
  # send a getaddr to seed peer discovery (used by the Phase 2 pool).
  defp become_ready(%{config: c, socket: socket, hs_timer: timer} = state, peer_version) do
    cancel_timer(timer)
    notify(state, {:ready, peer_version})
    c.transport.send(socket, Frame.encode(c.network, :getaddr, <<>>))
    %{state | phase: :ready, hs_timer: nil, peer_version: peer_version}
  end

  ## Helpers

  defp notify(%{config: %{owner: owner}}, {:ready, version}),
    do: send(owner, {:peer, self(), :ready, version})

  defp notify(%{config: %{owner: owner}}, {:frame, frame}),
    do: send(owner, {:peer, self(), :frame, frame})

  defp notify(%{config: %{owner: owner}}, {:down, reason}),
    do: send(owner, {:peer, self(), :down, reason})

  defp rearm(%{config: c, socket: socket} = state) do
    c.transport.setopts(socket, active: :once)
    state
  end

  defp connect_timeout(%Config{timeouts: t}), do: Map.get(t, :connect, @default_connect_timeout)

  defp handshake_timeout(%Config{timeouts: t}),
    do: Map.get(t, :handshake, @default_handshake_timeout)

  defp cancel_timer(nil), do: :ok
  defp cancel_timer(ref), do: Process.cancel_timer(ref)
end
