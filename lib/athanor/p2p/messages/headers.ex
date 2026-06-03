defmodule Athanor.P2P.Messages.Headers do
  @moduledoc """
  `getheaders` and `headers` message codecs.

  `getheaders` (request): `version::uint32-LE` + a CompactSize-prefixed block
  locator (≤ `MAX_LOCATOR_SZ` = 101 wire-order hashes) + a 32-byte stop hash
  (all-zero means "up to the tip").

  `headers` (response): a CompactSize count (≤ 2000) of entries, each an 80-byte
  block header followed by a CompactSize transaction count that is **always 0**
  in a headers message (a non-zero count is malformed).

  Pure (no IO).
  """

  alias Athanor.P2P.Codec.VarInt
  alias Athanor.P2P.Messages.BlockHeader

  @max_locator 101
  @max_headers 2000

  @doc """
  Serializes a `getheaders` request.

  ## Returns
    The wire `binary`, or `{:error, :too_many_locators}` if the locator exceeds
    `MAX_LOCATOR_SZ` (101).
  """
  @spec serialize_get_headers(non_neg_integer(), [<<_::256>>], <<_::256>>) ::
          binary() | {:error, :too_many_locators}
  def serialize_get_headers(_version, locator, _stop) when length(locator) > @max_locator do
    {:error, :too_many_locators}
  end

  def serialize_get_headers(version, locator, <<stop::binary-32>>) do
    <<version::little-32>> <>
      VarInt.write(length(locator)) <>
      Enum.map_join(locator, fn <<hash::binary-32>> -> hash end) <>
      stop
  end

  @doc """
  Parses a `headers` message body into a list of `BlockHeader` structs.

  ## Returns
    `{:ok, headers, rest}` | `:need_more` | `{:error, :bad_headers | :oversize}`.
  """
  @spec parse(binary()) ::
          {:ok, [BlockHeader.t()], binary()} | :need_more | {:error, :bad_headers | :oversize}
  def parse(binary) when is_binary(binary) do
    case VarInt.read(binary) do
      {:ok, count, _rest} when count > @max_headers -> {:error, :oversize}
      {:ok, count, rest} -> read_headers(rest, count, [])
      :need_more -> :need_more
    end
  end

  defp read_headers(rest, 0, acc), do: {:ok, Enum.reverse(acc), rest}

  defp read_headers(<<raw::binary-80, rest::binary>>, count, acc) do
    case VarInt.read(rest) do
      {:ok, 0, after_txcount} ->
        read_headers(after_txcount, count - 1, [%BlockHeader{raw: raw} | acc])

      {:ok, _nonzero, _} ->
        {:error, :bad_headers}

      :need_more ->
        :need_more
    end
  end

  defp read_headers(_incomplete, _count, _acc), do: :need_more
end
