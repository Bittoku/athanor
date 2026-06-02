defmodule Athanor.P2P.Codec.VarBytes do
  @moduledoc """
  Length-prefixed byte/string codec for the BSV P2P wire protocol.

  A var_bytes / var_str value is a `CompactSize` length prefix (see
  `Athanor.P2P.Codec.VarInt`) followed by exactly that many payload bytes.
  Strings on the wire are just UTF-8 byte payloads, so `write_str/1` and
  `read_str/2` are thin aliases over the byte forms.

  Pure (no IO). Readers compose over a streaming buffer: they return the
  unconsumed remainder, `:need_more` when the buffer is too short for the
  declared length, or `{:error, :oversize}` when a `:max` bound is exceeded
  (a defense against a malicious peer declaring a huge length).
  """

  alias Athanor.P2P.Codec.VarInt

  @doc """
  Encodes a byte payload as a CompactSize length prefix followed by the bytes.

  ## Parameters
    - `bytes` — the raw payload `binary`.

  ## Returns
    The length-prefixed `binary`.
  """
  @spec write_bytes(binary()) :: binary()
  def write_bytes(bytes) when is_binary(bytes), do: VarInt.write(byte_size(bytes)) <> bytes

  @doc """
  Encodes a string as a var_str (length-prefixed UTF-8 bytes).

  ## Parameters
    - `string` — the string to encode.

  ## Returns
    The length-prefixed `binary`.
  """
  @spec write_str(String.t()) :: binary()
  def write_str(string) when is_binary(string), do: write_bytes(string)

  @doc """
  Decodes a length-prefixed byte payload from the front of a buffer.

  ## Parameters
    - `binary` — a buffer beginning with a var_bytes value.
    - `opts` — options:
      - `:max` — reject (without buffering) any payload whose declared length
        exceeds this bound.

  ## Returns
    - `{:ok, payload, rest}` — the decoded bytes and the unconsumed remainder.
    - `:need_more` — the buffer is too short for the declared length.
    - `{:error, :oversize}` — the declared length exceeds `opts[:max]`.
  """
  @spec read_bytes(binary(), keyword()) ::
          {:ok, binary(), binary()} | :need_more | {:error, :oversize}
  def read_bytes(binary, opts \\ []) when is_binary(binary) do
    max = Keyword.get(opts, :max)

    case VarInt.read(binary) do
      {:ok, length, _rest} when is_integer(max) and length > max ->
        {:error, :oversize}

      {:ok, length, rest} ->
        case rest do
          <<payload::binary-size(length), tail::binary>> -> {:ok, payload, tail}
          _too_short -> :need_more
        end

      :need_more ->
        :need_more
    end
  end

  @doc """
  Decodes a var_str. Identical to `read_bytes/2` — the payload bytes are the string.
  """
  @spec read_str(binary(), keyword()) ::
          {:ok, String.t(), binary()} | :need_more | {:error, :oversize}
  def read_str(binary, opts \\ []), do: read_bytes(binary, opts)
end
