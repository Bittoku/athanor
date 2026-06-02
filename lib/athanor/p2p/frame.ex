defmodule Athanor.P2P.Frame do
  @moduledoc """
  BSV P2P message framing: encode/decode the 24-byte header + payload envelope.

  Wire layout (all integers little-endian):

      | offset | size | field    |
      |--------|------|----------|
      | 0      | 4    | magic    | network magic (see `Athanor.P2P.Network`)
      | 4      | 12   | command  | ASCII command, NUL-padded to 12 bytes
      | 16     | 4    | length   | payload byte count, little-endian uint32
      | 20     | 4    | checksum | first 4 bytes of double-SHA256(payload)
      | 24     | len  | payload  | message body

  `encode/3` is a pure function. `decode/3` is a streaming decoder: given a
  buffer that may hold zero, partial, or multiple frames, it returns the first
  complete frame plus the unconsumed remainder, `:need_more` when the buffer is
  too short, or `{:error, reason}` for a malformed frame the network layer
  should treat as a disconnect/banscore signal.

  This module is pure (no IO).
  """

  alias Athanor.P2P.Network
  alias Athanor.P2P.Codec.Hash

  @enforce_keys [:command, :payload]
  defstruct [:command, :payload]

  @type t :: %__MODULE__{command: String.t(), payload: binary()}

  @doc """
  Encodes a P2P message frame.

  ## Parameters
    - `network` — the `t:Athanor.P2P.Network.t/0` whose magic prefixes the frame.
    - `command` — a known command atom (see `Athanor.P2P.Network`).
    - `payload` — the message body `binary` (`<<>>` for empty messages).

  ## Returns
    The complete framed `binary` (24-byte header + payload). Raises
    `FunctionClauseError` on an unknown command.
  """
  @spec encode(Network.t(), atom(), binary()) :: binary()
  def encode(%Network{magic: magic}, command, payload) when is_binary(payload) do
    magic <>
      Network.padded_command(command) <>
      <<byte_size(payload)::little-32>> <>
      Hash.checksum4(payload) <>
      payload
  end
end
