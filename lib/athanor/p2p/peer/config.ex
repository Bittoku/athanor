defmodule Athanor.P2P.Peer.Config do
  @moduledoc """
  Configuration for a single `Athanor.P2P.Peer` connection (Phase 1, T1.3).

  Fields:

    * `:host` / `:port` — the remote node (host as a charlist for `:gen_tcp`).
    * `:network` — the `Athanor.P2P.Network` (magic, default port, seeds).
    * `:our_version` — the `Version` struct we advertise in the handshake.
    * `:transport` — a module implementing `Athanor.P2P.Transport`
      (defaults to `Transport.Gen`; tests inject `Transport.Fake`).
    * `:transport_opts` — options passed to `transport.connect/4`
      (socket options for `Gen`; `[fake: %{...}]` control for `Fake`).
    * `:owner` — the pid that receives `{:peer, pid, :ready|:frame|:down, _}`
      lifecycle messages. The peer is agnostic to who the owner is.
    * `:timeouts` — a map of millisecond timeouts: `:connect`, `:handshake`,
      and (Phase 1 later) `:ping_interval`, `:inactivity`.
  """

  alias Athanor.P2P.Network
  alias Athanor.P2P.Messages.Version
  alias Athanor.P2P.Transport

  @enforce_keys [:host, :port, :network, :our_version, :owner]
  defstruct host: nil,
            port: nil,
            network: nil,
            our_version: nil,
            transport: Transport.Gen,
            transport_opts: [],
            owner: nil,
            timeouts: %{}

  @type t :: %__MODULE__{
          host: charlist(),
          port: :inet.port_number(),
          network: Network.t(),
          our_version: Version.t(),
          transport: module(),
          transport_opts: keyword(),
          owner: pid(),
          timeouts: %{optional(atom()) => non_neg_integer()}
        }
end
