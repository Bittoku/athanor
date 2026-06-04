defmodule Athanor.P2P.Messages.BlockHeader do
  @moduledoc """
  An 80-byte Bitcoin block header, wrapped as a struct with derived helpers.

  The raw 80 bytes are: `version` (4) + `prev_block` (32) + `merkle_root` (32) +
  `timestamp` (4) + `bits` (4) + `nonce` (4). The block id is the double-SHA256
  of these 80 bytes.

  `hash/1` returns the **wire-order** id (raw double-SHA256 bytes); `prev_hash/1`
  returns the previous-block id in **display order** (byte-reversed), ready for
  the application/store layer. Pure (no IO).
  """

  alias Athanor.P2P.Codec.Hash

  @enforce_keys [:raw]
  defstruct [:raw]

  @type t :: %__MODULE__{raw: <<_::640>>}

  @doc "The wire-order block id: double-SHA256 of the 80 raw header bytes."
  @spec hash(t()) :: <<_::256>>
  def hash(%__MODULE__{raw: <<raw::binary-80>>}), do: Hash.double_sha256(raw)

  @doc "The previous-block id (header bytes 4..35) in display order."
  @spec prev_hash(t()) :: <<_::256>>
  def prev_hash(%__MODULE__{raw: <<_version::binary-4, prev::binary-32, _::binary>>}) do
    Hash.wire_to_display(prev)
  end
end
