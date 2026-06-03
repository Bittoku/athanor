defmodule Athanor.P2P.Messages.Reject do
  @moduledoc """
  `reject` message codec plus reason classification.

  Wire layout:

      | message | var_str  (the rejected command, e.g. "tx")        |
      | ccode   | uint8    (reject code)                            |
      | reason  | var_str  (human-readable reason)                  |
      | data    | 32 bytes (present only for "tx"/"block" rejects)  |

  `classify/1` collapses the `ccode` + `reason` into a coarse class
  (`:policy | :invalid | :conflicted | :transient | :unknown`) so the broadcast
  monitor can react to a peer's verdict without parsing free-text reasons.
  Pure (no IO).
  """

  alias Athanor.P2P.Codec.VarBytes

  @data_carrying ["tx", "block"]

  @enforce_keys [:message, :ccode, :reason]
  defstruct [:message, :ccode, :reason, data: <<>>]

  @type t :: %__MODULE__{
          message: String.t(),
          ccode: 0..255,
          reason: String.t(),
          data: binary()
        }

  @type reject_class :: :policy | :invalid | :conflicted | :transient | :unknown

  @doc "Serializes a `reject` message to its wire payload."
  @spec serialize(t()) :: binary()
  def serialize(%__MODULE__{} = r) do
    VarBytes.write_str(r.message) <> <<r.ccode>> <> VarBytes.write_str(r.reason) <> r.data
  end

  @doc """
  Parses a `reject` payload. A "tx"/"block" reject carries a trailing 32-byte
  hash; others have empty `data`. Returns `{:ok, %Reject{}, rest}` or `:need_more`.
  """
  @spec parse(binary()) :: {:ok, t(), binary()} | :need_more
  def parse(binary) when is_binary(binary) do
    with {:ok, message, after_msg} <- VarBytes.read_str(binary),
         <<ccode, after_code::binary>> <- after_msg,
         {:ok, reason, after_reason} <- VarBytes.read_str(after_code) do
      take_data(message, ccode, reason, after_reason)
    else
      _ -> :need_more
    end
  end

  defp take_data(message, ccode, reason, rest) when message in @data_carrying do
    case rest do
      <<data::binary-32, tail::binary>> ->
        {:ok, %__MODULE__{message: message, ccode: ccode, reason: reason, data: data}, tail}

      _ ->
        :need_more
    end
  end

  defp take_data(message, ccode, reason, rest) do
    {:ok, %__MODULE__{message: message, ccode: ccode, reason: reason, data: <<>>}, rest}
  end

  @doc """
  Classifies a reject into a coarse terminal class for the broadcast monitor.
  """
  @spec classify(t()) :: reject_class()
  def classify(%__MODULE__{ccode: ccode, reason: reason}) do
    cond do
      # REJECT_NONSTANDARD / REJECT_DUST / REJECT_INSUFFICIENTFEE
      ccode in [0x40, 0x41, 0x42] -> :policy
      # REJECT_MALFORMED / REJECT_INVALID
      ccode in [0x01, 0x10] -> :invalid
      # REJECT_DUPLICATE, or any conflict reason
      ccode == 0x12 or String.contains?(reason, "conflict") -> :conflicted
      # REJECT_OBSOLETE
      ccode == 0x11 -> :transient
      true -> :unknown
    end
  end
end
