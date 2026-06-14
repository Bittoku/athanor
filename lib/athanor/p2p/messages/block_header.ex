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

  @doc """
  The previous-block id (header bytes 4..35) in **wire/internal order** — the raw
  `prev_block` bytes, *not* reversed. This is the parent-link convention the
  headers chain keys on (it matches `hash/1`); display-order `prev_hash/1` is for
  the store boundary only.
  """
  @spec prev_hash_wire(t()) :: <<_::256>>
  def prev_hash_wire(%__MODULE__{raw: <<_version::binary-4, prev::binary-32, _::binary>>}),
    do: prev

  @doc """
  The compact difficulty target (`nBits`) as a `uint32`, read from the 4-byte
  little-endian `bits` field (header bytes 72..75).
  """
  @spec bits(t()) :: 0..0xFFFFFFFF
  def bits(%__MODULE__{raw: <<_::binary-72, bits::little-32, _nonce::binary-4>>}), do: bits

  @doc """
  The block `timestamp` (Unix seconds) as a `uint32`, read from the 4-byte
  little-endian field at header bytes 68..71. Phase 7 F7.1 needs this for the
  cw-144 DAA window; the value already lives in the retained 80-byte header.
  """
  @spec timestamp(t()) :: 0..0xFFFFFFFF
  def timestamp(%__MODULE__{raw: <<_::binary-68, ts::little-32, _::binary-8>>}), do: ts
end
