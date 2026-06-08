defmodule Athanor.P2P.Transport do
  @moduledoc """
  Injectable TCP transport behaviour for the P2P peer (Phase 1, T1.0).

  The `Peer` GenServer never calls `:gen_tcp` directly; it goes through a module
  implementing this behaviour. Production uses `Athanor.P2P.Transport.Gen` (a thin
  `:gen_tcp` wrapper); tests use `Athanor.P2P.Transport.Fake`, a deterministic
  double that captures outbound bytes and lets a test inject inbound data. This
  keeps ~all of the peer's protocol logic unit-testable without real sockets.

  ## Active mode

  Implementations operate the socket in **active-once** mode: inbound data is
  delivered to the *controlling process* (the process that called `connect/4`)
  as ordinary messages, never via a blocking `recv`:

    * `{:tcp, socket, data :: binary}` — a chunk of inbound bytes.
    * `{:tcp_closed, socket}` — the peer closed the connection.
    * `{:tcp_error, socket, reason}` — a socket error.

  After each `{:tcp, ...}` the socket stops delivering until the owner re-arms it
  with `setopts(socket, active: :once)`, which bounds the mailbox (back-pressure).
  """

  @typedoc "Opaque transport handle (a `:gen_tcp` socket, or a Fake handle in tests)."
  @type socket :: term()

  @typedoc "Host as a charlist, as `:gen_tcp` expects."
  @type host :: charlist()

  @doc """
  Opens a connection to `host`:`port`.

  `opts` are transport options (socket options for `Gen`; control options such
  as `:fake` for `Fake`). `timeout` is the connect timeout in milliseconds.
  Returns `{:ok, socket}` or `{:error, reason}`. The calling process becomes the
  controlling process that receives active-mode messages.
  """
  @callback connect(host(), :inet.port_number(), keyword(), timeout()) ::
              {:ok, socket()} | {:error, term()}

  @doc "Sends `data` on the socket. Returns `:ok` or `{:error, reason}`."
  @callback send(socket(), iodata()) :: :ok | {:error, term()}

  @doc "Sets socket options (notably `active: :once` to re-arm receive)."
  @callback setopts(socket(), keyword()) :: :ok | {:error, term()}

  @doc "Closes the socket. Idempotent — always returns `:ok`."
  @callback close(socket()) :: :ok
end
