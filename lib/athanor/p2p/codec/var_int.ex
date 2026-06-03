defmodule Athanor.P2P.Codec.VarInt do
  @moduledoc """
  Bitcoin CompactSize ("varint") integer codec for the BSV P2P wire protocol.

  CompactSize encodes an unsigned integer in the fewest bytes, using a leading
  marker byte to indicate width for values >= 0xFD:

    * `n < 0xFD`        -> 1 byte: `n`
    * `n <= 0xFFFF`     -> `0xFD` + `n` as little-endian uint16
    * `n <= 0xFFFFFFFF` -> `0xFE` + `n` as little-endian uint32
    * otherwise         -> `0xFF` + `n` as little-endian uint64

  This module is pure (no IO). `read/1` composes over a streaming buffer by
  returning the unconsumed remainder, or `:need_more` when the buffer is too
  short to decode a complete value.
  """

  @doc """
  Encodes a non-negative integer as a CompactSize binary.

  ## Parameters
    - `value` — a non-negative integer (`0..0xFFFFFFFFFFFFFFFF`).

  ## Returns
    The CompactSize-encoded `binary`, using the smallest valid width.
  """
  @spec write(non_neg_integer()) :: binary()
  def write(value) when value < 0xFD, do: <<value>>
  def write(value) when value <= 0xFFFF, do: <<0xFD, value::little-16>>
  def write(value) when value <= 0xFFFFFFFF, do: <<0xFE, value::little-32>>
  def write(value) when value <= 0xFFFFFFFFFFFFFFFF, do: <<0xFF, value::little-64>>

  @doc """
  Decodes a CompactSize value from the front of a binary buffer.

  ## Parameters
    - `binary` — a buffer beginning with a CompactSize-encoded integer.

  ## Returns
    - `{:ok, value, rest}` — the decoded integer and the unconsumed remainder.
    - `:need_more` — the buffer is too short to hold a complete value.
  """
  @spec read(binary()) :: {:ok, non_neg_integer(), binary()} | :need_more
  def read(<<0xFF, value::little-64, rest::binary>>), do: {:ok, value, rest}
  def read(<<0xFE, value::little-32, rest::binary>>), do: {:ok, value, rest}
  def read(<<0xFD, value::little-16, rest::binary>>), do: {:ok, value, rest}
  def read(<<value, rest::binary>>) when value < 0xFD, do: {:ok, value, rest}
  # Marker present but not enough trailing bytes, or empty input.
  def read(_buffer), do: :need_more
end
