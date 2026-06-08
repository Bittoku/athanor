defmodule Athanor.P2P.Transport.Fake do
  @moduledoc """
  Deterministic test double for `Athanor.P2P.Transport` (T1.0 test support).

  Instead of a real socket, a Fake handle is an `Agent` holding the controlling
  process (the connector), a capture of all outbound byte chunks, and a log of
  `setopts/2` calls. Tests drive the "wire" synchronously:

    * `sent/1` — outbound chunks in send order (assert what the peer sent).
    * `deliver/2` / `deliver_closed/1` / `deliver_error/2` — inject the
      active-mode `{:tcp, _}` / `{:tcp_closed, _}` / `{:tcp_error, _}` messages.
    * `setopts_log/1` — every `setopts` call (assert active-once re-arming).

  `connect/4` reads control options from `opts[:fake]`:

    * `:test` — a pid that receives `{:fake_handle, socket}` so a test can grab
      the handle the *peer* created (the peer connects in its own process).
    * `:refuse` — when truthy, `connect/4` returns `{:error, :econnrefused}`.

  The controlling process (recipient of injected messages) is whoever calls
  `connect/4`, mirroring `:gen_tcp` active-mode semantics.
  """

  @behaviour Athanor.P2P.Transport

  @doc """
  "Connects": starts the Fake handle bound to the calling process, or refuses.

  Returns `{:ok, socket}` (the handle), or `{:error, :econnrefused}` when
  `opts[:fake][:refuse]` is truthy.
  """
  @impl true
  def connect(_host, _port, opts, _timeout) do
    fake = Keyword.get(opts, :fake, %{})

    if Map.get(fake, :refuse) do
      {:error, :econnrefused}
    else
      controlling = self()

      {:ok, socket} =
        Agent.start_link(fn ->
          %{controlling: controlling, sent: [], setopts: [], closed: false}
        end)

      if test = Map.get(fake, :test), do: Kernel.send(test, {:fake_handle, socket})

      {:ok, socket}
    end
  end

  @doc "Captures `data` (as a binary) in send order. Always `:ok`."
  @impl true
  def send(socket, data) do
    bin = IO.iodata_to_binary(data)
    Agent.update(socket, fn st -> %{st | sent: [bin | st.sent]} end)
    :ok
  end

  @doc "Records the `setopts` call (for active-once assertions). Always `:ok`."
  @impl true
  def setopts(socket, opts) do
    Agent.update(socket, fn st -> %{st | setopts: [opts | st.setopts]} end)
    :ok
  end

  @doc "Marks the handle closed. Idempotent — always `:ok`."
  @impl true
  def close(socket) do
    if Process.alive?(socket) do
      Agent.update(socket, fn st -> %{st | closed: true} end)
    end

    :ok
  end

  ## Test-driving API (not part of the behaviour)

  @doc "Outbound byte chunks captured by `send/2`, in send order."
  def sent(socket), do: socket |> Agent.get(& &1.sent) |> Enum.reverse()

  @doc "Every `setopts/2` argument list, in call order."
  def setopts_log(socket), do: socket |> Agent.get(& &1.setopts) |> Enum.reverse()

  @doc "Whether `close/1` has been called on this handle."
  def closed?(socket), do: Agent.get(socket, & &1.closed)

  @doc "Injects an inbound `{:tcp, socket, bytes}` message to the controlling process."
  def deliver(socket, bytes) do
    controlling = Agent.get(socket, & &1.controlling)
    Kernel.send(controlling, {:tcp, socket, bytes})
    :ok
  end

  @doc "Injects an inbound `{:tcp_closed, socket}` message."
  def deliver_closed(socket) do
    controlling = Agent.get(socket, & &1.controlling)
    Kernel.send(controlling, {:tcp_closed, socket})
    :ok
  end

  @doc "Injects an inbound `{:tcp_error, socket, reason}` message."
  def deliver_error(socket, reason) do
    controlling = Agent.get(socket, & &1.controlling)
    Kernel.send(controlling, {:tcp_error, socket, reason})
    :ok
  end
end
