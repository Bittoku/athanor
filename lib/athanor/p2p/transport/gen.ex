defmodule Athanor.P2P.Transport.Gen do
  @moduledoc """
  Production `:gen_tcp` implementation of `Athanor.P2P.Transport` (T1.0).

  A thin, dependency-free wrapper. Sockets are opened in binary, raw-packet,
  active-once, nodelay mode so the owning `Peer` receives inbound bytes as
  `{:tcp, socket, data}` messages with bounded mailbox pressure.
  """

  @behaviour Athanor.P2P.Transport

  # Caller-supplied opts override these per-key.
  @default_opts [:binary, active: :once, nodelay: true, packet: :raw]

  @doc """
  Connects via `:gen_tcp.connect/4`. `opts` are merged over the active-once
  defaults (caller wins per key). Returns `{:ok, socket}` or `{:error, reason}`.
  """
  @impl true
  def connect(host, port, opts, timeout) do
    :gen_tcp.connect(host, port, merge_opts(opts), timeout)
  end

  @doc "Sends `data` via `:gen_tcp.send/2`."
  @impl true
  def send(socket, data), do: :gen_tcp.send(socket, data)

  @doc "Re-arms / configures the socket via `:inet.setopts/2`."
  @impl true
  def setopts(socket, opts), do: :inet.setopts(socket, opts)

  @doc "Closes via `:gen_tcp.close/1` (always `:ok`)."
  @impl true
  def close(socket), do: :gen_tcp.close(socket)

  # Merge caller opts over the defaults. `:binary` is a bare atom flag, so we
  # merge the keyword pairs and re-prepend `:binary` unless the caller disabled
  # it explicitly via `mode:`/`:list`.
  defp merge_opts(opts) do
    {flags, kw} = Enum.split_with(@default_opts, &is_atom/1)
    merged = Keyword.merge(kw, Keyword.new(opts |> Enum.reject(&is_atom/1)))
    extra_flags = Enum.filter(opts, &is_atom/1)
    Enum.uniq(flags ++ extra_flags) ++ merged
  end
end
