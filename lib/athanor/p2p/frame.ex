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

  # Default inbound payload ceiling: 32 MiB, large enough for big BSV mainnet
  # txs (matches the protoconf max-recv we advertise) yet bounded.
  @default_max_payload 32 * 1024 * 1024

  @doc """
  Decodes the first complete frame from the front of a streaming buffer.

  Checks are ordered cheapest-first (magic, then size) so a hostile or
  misframed peer is rejected before any hashing work.

  ## Parameters
    - `network` — the expected `t:Athanor.P2P.Network.t/0` (its magic must match).
    - `buffer` — accumulated inbound bytes (zero, partial, or multiple frames).
    - `opts` — options:
      - `:max_payload` — reject (without buffering) any frame whose declared
        length exceeds this bound (default 32 MiB).

  ## Returns
    - `{:ok, %Frame{}, rest}` — the first frame and the unconsumed remainder.
    - `:need_more` — the buffer does not yet hold a complete frame.
    - `{:error, :bad_magic | :oversized_payload | :bad_command | :bad_checksum}`.
  """
  @spec decode(Network.t(), binary(), keyword()) ::
          {:ok, t(), binary()}
          | :need_more
          | {:error, :bad_magic | :oversized_payload | :bad_command | :bad_checksum}
  def decode(network, buffer, opts \\ [])

  def decode(%Network{magic: magic}, buffer, opts) when is_binary(buffer) do
    max_payload = Keyword.get(opts, :max_payload, @default_max_payload)

    case buffer do
      <<^magic::binary-4, command::binary-12, length::little-32, checksum::binary-4,
        rest::binary>> ->
        decode_body(command, length, checksum, rest, max_payload)

      # Enough bytes to judge the magic, and it is wrong → fatal.
      <<other_magic::binary-4, _::binary>> when other_magic != magic ->
        {:error, :bad_magic}

      # Fewer than a full header (with the right magic prefix), or < 4 bytes.
      _too_short ->
        :need_more
    end
  end

  defp decode_body(_command, length, _checksum, _rest, max_payload) when length > max_payload do
    {:error, :oversized_payload}
  end

  defp decode_body(command, length, checksum, rest, _max_payload) do
    case rest do
      <<payload::binary-size(length), tail::binary>> ->
        validate(command, checksum, payload, tail)

      _incomplete ->
        :need_more
    end
  end

  defp validate(command, checksum, payload, tail) do
    with {:ok, name} <- parse_command(command),
         true <- checksum == Hash.checksum4(payload) || {:error, :bad_checksum} do
      {:ok, %__MODULE__{command: name, payload: payload}, tail}
    else
      :error -> {:error, :bad_command}
      {:error, :bad_checksum} -> {:error, :bad_checksum}
    end
  end

  # A valid command field is a non-empty printable-ASCII name, NUL-padded: every
  # byte after the first NUL must also be NUL.
  defp parse_command(field) do
    {name, padding} =
      case :binary.split(field, <<0>>) do
        [name] -> {name, <<>>}
        [name, padding] -> {name, padding}
      end

    if name != "" and printable_ascii?(name) and all_zero?(padding) do
      {:ok, name}
    else
      :error
    end
  end

  defp printable_ascii?(binary), do: for(<<b <- binary>>, do: b) |> Enum.all?(&(&1 in 0x20..0x7E))

  defp all_zero?(binary), do: for(<<b <- binary>>, do: b) |> Enum.all?(&(&1 == 0))
end
