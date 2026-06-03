defmodule Athanor.P2P.Messages.Inv do
  @moduledoc """
  Inventory-vector list codec, shared by the `inv`, `getdata`, and `notfound`
  messages (identical wire bodies — only the frame command differs).

  Wire layout:

      | count | CompactSize varint                          |
      | items | count × (type::uint32-LE  ++  hash::32 bytes) |

  Each item is an `{type, hash}` tuple. `type` is `:tx` (1) or `:block` (2);
  unknown integer codes pass through unchanged so the codec round-trips any
  inventory type without acting on it. Hashes are kept in **wire order** (raw,
  unreversed) — order conversion happens only at the P2P boundary.

  `parse/2` accepts a `:max_items` bound and rejects an over-large declared
  count before reading any items (a malicious-peer guard). Pure (no IO).
  """

  alias Athanor.P2P.Codec.VarInt

  @type inv_type :: :tx | :block | non_neg_integer()
  @type item :: {inv_type(), <<_::256>>}

  @doc "Maps an inventory type to its wire code. Integers pass through; unknown atoms raise."
  @spec type_code(inv_type()) :: non_neg_integer()
  def type_code(:tx), do: 1
  def type_code(:block), do: 2
  def type_code(code) when is_integer(code) and code >= 0, do: code

  @doc "Maps a wire code to its type atom. Unknown codes pass through as the integer."
  @spec type_from_code(non_neg_integer()) :: inv_type()
  def type_from_code(1), do: :tx
  def type_from_code(2), do: :block
  def type_from_code(code) when is_integer(code), do: code

  @doc "Serializes a list of inventory items to a count-prefixed wire body."
  @spec serialize([item()]) :: binary()
  def serialize(items) when is_list(items) do
    body =
      Enum.map_join(items, fn {type, <<hash::binary-32>>} ->
        <<type_code(type)::little-32>> <> hash
      end)

    VarInt.write(length(items)) <> body
  end

  @doc """
  Parses an inventory body.

  ## Parameters
    - `binary` — buffer beginning with an inv/getdata/notfound body.
    - `opts` — `:max_items` rejects an over-large declared count.

  ## Returns
    `{:ok, items, rest}` | `:need_more` | `{:error, :oversize}`.
  """
  @spec parse(binary(), keyword()) :: {:ok, [item()], binary()} | :need_more | {:error, :oversize}
  def parse(binary, opts \\ []) when is_binary(binary) do
    max_items = Keyword.get(opts, :max_items)

    case VarInt.read(binary) do
      {:ok, count, _rest} when is_integer(max_items) and count > max_items ->
        {:error, :oversize}

      {:ok, count, rest} ->
        read_items(rest, count, [])

      :need_more ->
        :need_more
    end
  end

  defp read_items(rest, 0, acc), do: {:ok, Enum.reverse(acc), rest}

  defp read_items(<<code::little-32, hash::binary-32, rest::binary>>, count, acc) do
    read_items(rest, count - 1, [{type_from_code(code), hash} | acc])
  end

  defp read_items(_incomplete, _count, _acc), do: :need_more
end
