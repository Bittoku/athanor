defmodule Athanor.P2P.Transport.LoopbackRewrite do
  @moduledoc """
  Test-support transport that rewrites a connect target before delegating to the
  real `Transport.Gen` (Phase 2, T2.6).

  The pool integration test wants distinct **/24** synthetic addresses (so the
  address book's diversity logic is genuinely exercised) while the actual
  sockets all connect to `127.0.0.1` on different ephemeral ports (the
  `FakePeerServer`s). This transport reads a `rewrite` map from `transport_opts`
  — `%{synthetic_host_charlist => {real_host, real_port}}` — and translates the
  dial, leaving everything else identical to `Transport.Gen`. No production code
  is touched; the pool simply injects `transport: LoopbackRewrite`.
  """

  @behaviour Athanor.P2P.Transport

  alias Athanor.P2P.Transport.Gen

  @impl true
  def connect(host, port, opts, timeout) do
    {rhost, rport} =
      case Map.get(Keyword.get(opts, :rewrite, %{}), host) do
        {real_host, real_port} -> {real_host, real_port}
        nil -> {host, port}
      end

    Gen.connect(rhost, rport, Keyword.delete(opts, :rewrite), timeout)
  end

  @impl true
  def send(socket, data), do: Gen.send(socket, data)

  @impl true
  def setopts(socket, opts), do: Gen.setopts(socket, opts)

  @impl true
  def close(socket), do: Gen.close(socket)
end
