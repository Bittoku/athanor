defmodule Athanor.P2P.Messages.Addr do
  @moduledoc """
  `addr` (peer gossip) message codec.

  Wire layout:

      | count | CompactSize varint                                          |
      | items | count × (time::uint32-LE  ++  26-byte net-addr)             |

  Unlike the net-addr fields inside a `version` message, each `addr` entry is
  prefixed with a 4-byte `time`. The 26-byte net-addr is
  `services::uint64-LE` + `ip::16 bytes` + `port::uint16-BIG-endian`.

  Each entry is a `{time, services, ip, port}` tuple (`ip` is the 16-byte form).
  `ipv4_to_16/1` / `ipv4_from_16/1` convert between a 4-byte IPv4 address and its
  IPv4-mapped IPv6 (`::ffff:a.b.c.d`) wire form. Count is capped at 1000. Pure (no IO).
  """

  alias Athanor.P2P.Codec.VarInt

  @max_addr 1000
  @v4_prefix <<0::80, 0xFFFF::16>>

  @type entry ::
          {time :: non_neg_integer(), services :: non_neg_integer(), ip :: <<_::128>>,
           port :: non_neg_integer()}

  @doc "Maps a 4-byte IPv4 address to its 16-byte IPv4-mapped IPv6 form."
  @spec ipv4_to_16(<<_::32>>) :: <<_::128>>
  def ipv4_to_16(<<_a, _b, _c, _d>> = v4), do: @v4_prefix <> v4

  @doc "Extracts the 4-byte IPv4 address from a mapped form, or `:error` if not mapped."
  @spec ipv4_from_16(<<_::128>>) :: {:ok, <<_::32>>} | :error
  def ipv4_from_16(<<@v4_prefix, v4::binary-4>>), do: {:ok, v4}
  def ipv4_from_16(<<_::binary-16>>), do: :error

  @doc "Serializes a list of `addr` entries to a count-prefixed wire body."
  @spec serialize([entry()]) :: binary()
  def serialize(entries) when is_list(entries) do
    body =
      Enum.map_join(entries, fn {time, services, <<ip::binary-16>>, port} ->
        <<time::little-32, services::little-64, ip::binary-16, port::big-16>>
      end)

    VarInt.write(length(entries)) <> body
  end

  @doc """
  Parses an `addr` body. Returns `{:ok, entries, rest}` | `:need_more` |
  `{:error, :oversize}` (count beyond the 1000 cap).
  """
  @spec parse(binary()) :: {:ok, [entry()], binary()} | :need_more | {:error, :oversize}
  def parse(binary) when is_binary(binary) do
    case VarInt.read(binary) do
      {:ok, count, _rest} when count > @max_addr -> {:error, :oversize}
      {:ok, count, rest} -> read_entries(rest, count, [])
      :need_more -> :need_more
    end
  end

  defp read_entries(rest, 0, acc), do: {:ok, Enum.reverse(acc), rest}

  defp read_entries(
         <<time::little-32, services::little-64, ip::binary-16, port::big-16, rest::binary>>,
         count,
         acc
       ) do
    read_entries(rest, count - 1, [{time, services, ip, port} | acc])
  end

  defp read_entries(_incomplete, _count, _acc), do: :need_more
end
