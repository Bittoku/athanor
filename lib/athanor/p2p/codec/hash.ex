defmodule Athanor.P2P.Codec.Hash do
  @moduledoc """
  Hashing and hash-byte-order helpers for the BSV P2P wire protocol.

  Bitcoin uses double-SHA256 (`SHA256(SHA256(x))`) for message checksums and
  for block/transaction identifiers. Frame checksums are the first four bytes
  of the double hash of the payload.

  ## Byte order
  Hashes travel on the P2P wire in **wire order** — the raw byte output of
  double-SHA256. Block explorers, REST APIs, and Athanor's own stores use
  **display order**, which is the byte-reversed form. `wire_to_display/1` and
  `display_to_wire/1` convert between them and must be applied **only** at the
  P2P boundary so the rest of the system sees a single canonical order.

  Pure (no IO).
  """

  @doc """
  Computes `SHA256(SHA256(data))`.

  ## Parameters
    - `data` — the input `binary`.

  ## Returns
    The 32-byte double-SHA256 digest (wire order).
  """
  @spec double_sha256(binary()) :: binary()
  def double_sha256(data) when is_binary(data) do
    :crypto.hash(:sha256, :crypto.hash(:sha256, data))
  end

  @doc """
  Returns the 4-byte frame checksum for a payload: the first four bytes of its
  double-SHA256 digest.

  ## Parameters
    - `payload` — the message payload `binary`.

  ## Returns
    A 4-byte `binary`.
  """
  @spec checksum4(binary()) :: binary()
  def checksum4(payload) when is_binary(payload) do
    binary_part(double_sha256(payload), 0, 4)
  end

  @doc """
  Converts a 32-byte wire-order hash to display order (byte-reversed).

  ## Parameters
    - `hash` — a 32-byte wire-order hash.

  ## Returns
    The 32-byte display-order hash.
  """
  @spec wire_to_display(<<_::256>>) :: <<_::256>>
  def wire_to_display(<<hash::binary-size(32)>>), do: reverse_bytes(hash)

  @doc """
  Converts a 32-byte display-order hash to wire order (byte-reversed).
  Inverse of `wire_to_display/1`.

  ## Parameters
    - `hash` — a 32-byte display-order hash.

  ## Returns
    The 32-byte wire-order hash.
  """
  @spec display_to_wire(<<_::256>>) :: <<_::256>>
  def display_to_wire(<<hash::binary-size(32)>>), do: reverse_bytes(hash)

  defp reverse_bytes(binary) do
    binary |> :binary.bin_to_list() |> Enum.reverse() |> :binary.list_to_bin()
  end
end
