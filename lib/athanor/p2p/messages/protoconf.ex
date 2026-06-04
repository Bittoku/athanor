defmodule Athanor.P2P.Messages.Protoconf do
  @moduledoc """
  BSV-specific `protoconf` message codec.

  `protoconf` is sent right after our `verack` to advertise the maximum message
  payload this node is willing to receive (BSV raised this far above Bitcoin's
  legacy limit to allow large transactions and blocks).

  Wire layout:

      | numberOfFields      | CompactSize varint           |
      | maxRecvPayloadLength | uint32 LE                   |
      | streamPolicies      | var_str (only if fields >= 2) |

  `numberOfFields` is 2 when stream policies are present, 1 otherwise. The
  default `max_recv_payload` is 32 MiB — the big-tx ceiling the frame decoder
  also enforces. This module is pure (no IO).
  """

  alias Athanor.P2P.Codec.{VarBytes, VarInt}

  @default_max_recv 33_554_432

  defstruct max_recv_payload: @default_max_recv, policies: "Default"

  @type t :: %__MODULE__{max_recv_payload: non_neg_integer(), policies: String.t()}

  @doc "Serializes a `protoconf` message to its wire payload."
  @spec serialize(t()) :: binary()
  def serialize(%__MODULE__{policies: ""} = p) do
    VarInt.write(1) <> <<p.max_recv_payload::little-32>>
  end

  def serialize(%__MODULE__{} = p) do
    VarInt.write(2) <> <<p.max_recv_payload::little-32>> <> VarBytes.write_str(p.policies)
  end

  @doc """
  Parses a `protoconf` payload. A one-field message yields `policies: ""`.
  Returns `{:ok, %Protoconf{}, rest}` or `:need_more`.
  """
  @spec parse(binary()) :: {:ok, t(), binary()} | :need_more
  def parse(binary) when is_binary(binary) do
    with {:ok, fields, after_count} <- VarInt.read(binary),
         <<max_recv::little-32, after_max::binary>> <- after_count do
      parse_policies(fields, max_recv, after_max)
    else
      _ -> :need_more
    end
  end

  defp parse_policies(fields, max_recv, after_max) when fields >= 2 do
    case VarBytes.read_str(after_max) do
      {:ok, policies, tail} ->
        {:ok, %__MODULE__{max_recv_payload: max_recv, policies: policies}, tail}

      _ ->
        :need_more
    end
  end

  defp parse_policies(_fields, max_recv, after_max) do
    {:ok, %__MODULE__{max_recv_payload: max_recv, policies: ""}, after_max}
  end
end
