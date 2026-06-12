defmodule Athanor.P2P.PeerPool.Config do
  @moduledoc """
  Configuration for `Athanor.P2P.PeerPool` (Phase 2, T2.3) — the single,
  canonical pool config.

  Fields:

    * `:network` — the `Athanor.P2P.Network` to bootstrap and dial on.
    * `:target` — desired number of healthy peers (default 8).
    * `:our_version` — the `Version` advertised in each peer handshake.
    * `:peer_starter` — `fn Peer.Config -> {:ok, pid} | {:error, reason}`
      (default `&Athanor.P2P.Peer.start_link/1`; tests inject a fake).
    * `:resolver` — DNS resolver `fn host -> {:ok, [ip4]} | {:error, _}` used by
      `Discovery` at bootstrap (default wraps `:inet.getaddrs/2`).
    * `:transport` / `:transport_opts` — forwarded into each child `Peer.Config`.
    * `:seeds` — explicit `[{ip, port}]` override seeded into the address book at
      init; **unioned with** DNS/fallback discovery, never a replacement.
    * `:cooldown_ms` — negative cooldown applied to a failed/dropped address
      before it is eligible to redial (default 15 min).
    * `:now_fun` — `fn -> integer_ms` clock (default monotonic ms); injected in
      tests to drive cooldown deterministically.
  """

  alias Athanor.P2P.Network
  alias Athanor.P2P.Messages.Version
  alias Athanor.P2P.Transport

  @enforce_keys [:network, :our_version]
  defstruct network: nil,
            target: 8,
            our_version: nil,
            peer_starter: nil,
            resolver: nil,
            transport: Transport.Gen,
            transport_opts: [],
            seeds: [],
            cooldown_ms: 900_000,
            now_fun: nil,
            frame_sink: nil

  @type addr :: {{byte(), byte(), byte(), byte()}, :inet.port_number()}
  @type t :: %__MODULE__{
          network: Network.t(),
          target: pos_integer(),
          our_version: Version.t(),
          peer_starter: (term() -> {:ok, pid()} | {:error, term()}) | nil,
          resolver: (String.t() -> {:ok, list()} | {:error, term()}) | nil,
          transport: module(),
          transport_opts: keyword(),
          seeds: [addr()],
          cooldown_ms: non_neg_integer(),
          now_fun: (-> integer()) | nil,
          frame_sink: pid() | nil
        }
end
