defmodule Athanor.P2P.HeadersChain.Work do
  @moduledoc """
  Phase 6 T6.1 (§A) — the pure, security-critical numeric core of the headers
  chain: decode a header's compact `bits` (Bitcoin **nBits**) into a 256-bit
  `target`, derive the per-header proof-of-work, and verify a block hash meets its
  target.

  ## nBits / compact form
  The 4-byte `bits` field, read as a `uint32`, is `(exponent << 24) | mantissa`
  where `exponent` is the high byte and `mantissa` the low 24 bits. The target is
  `mantissa * 256^(exponent - 3)` (a right-shift when `exponent < 3`). A compact
  whose mantissa has the sign bit (`0x800000`) set is **negative**, a zero
  mantissa gives a **zero** target, and a too-large exponent **overflows** 2^256 —
  all three are rejected (`:error`) so a peer cannot feed a malformed `bits` to
  credit bogus work.

  ## Work
  `work = floor(2^256 / (target + 1))` — the standard expected number of hashes to
  find one block at that target. Per-header work feeds the headers tree's
  cumulative-work tip selection.

  ## PoW check
  `meets_target?/2` interprets the **wire/internal-order** block hash as a
  little-endian 256-bit integer and returns whether it is `≤ target`. This is the
  trust floor that makes cumulative-work comparison safe against cheap forgery: a
  header whose hash does not meet its own claimed target is rejected by the tree.

  `valid_pow?/3` adds the **consensus pow-limit gate**: a header is only credited
  if its claimed `target` is at or below the network's maximum target (`bits` is
  at least minimum difficulty) *and* its hash meets that target. Without the limit
  a peer could advertise an easier-than-consensus `bits` (a huge but
  non-overflowing target) that a real node would reject, and drive the
  P2P-primary tip/reorg machinery — so for the production chain the pow-limit
  check must fail closed at the header-validation boundary.

  Pure (no IO).
  """

  import Bitwise

  @two_256 Integer.pow(2, 256)

  @type compact :: 0..0xFFFFFFFF

  @doc """
  Decodes a compact `bits` value to its 256-bit target.

  ## Returns
    `{:ok, target}` (a positive integer `< 2^256`), or `:error` for a negative,
    zero, or overflowing compact.
  """
  @spec compact_to_target(compact()) :: {:ok, pos_integer()} | :error
  def compact_to_target(compact)
      when is_integer(compact) and compact >= 0 and compact <= 0xFFFFFFFF do
    exponent = compact >>> 24
    mantissa = compact &&& 0xFFFFFF

    cond do
      (mantissa &&& 0x800000) != 0 -> :error
      mantissa == 0 -> :error
      exponent <= 3 -> nonzero(mantissa >>> (8 * (3 - exponent)))
      true -> bounded(mantissa <<< (8 * (exponent - 3)))
    end
  end

  def compact_to_target(_), do: :error

  @doc """
  The per-header work `floor(2^256 / (target + 1))`, or `:error` for a malformed
  compact.
  """
  @spec work(compact()) :: {:ok, pos_integer()} | :error
  def work(compact) do
    case compact_to_target(compact) do
      {:ok, target} -> {:ok, div(@two_256, target + 1)}
      :error -> :error
    end
  end

  @doc """
  Whether a wire/internal-order block `hash` (little-endian 256-bit integer) meets
  the target encoded by `compact`. `false` for a malformed compact — a header is
  never trusted on bad `bits`.
  """
  @spec meets_target?(<<_::256>>, compact()) :: boolean()
  def meets_target?(<<hash::binary-32>>, compact) do
    case compact_to_target(compact) do
      {:ok, target} -> :binary.decode_unsigned(hash, :little) <= target
      :error -> false
    end
  end

  @doc """
  Consensus proof-of-work check for a header: the claimed target (from `compact`)
  must be at or below the network pow-limit (`limit_compact`), and the
  wire/internal-order `hash` must meet that target.

  Returns `false` for an over-limit (easier-than-consensus) target, a hash that
  does not meet its target, or a malformed compact — a header is never credited on
  a `bits` an actual node would reject.
  """
  @spec valid_pow?(<<_::256>>, compact(), compact()) :: boolean()
  def valid_pow?(<<_::binary-32>> = hash, compact, limit_compact) do
    with {:ok, target} <- compact_to_target(compact),
         {:ok, limit} <- compact_to_target(limit_compact),
         true <- target <= limit do
      :binary.decode_unsigned(hash, :little) <= target
    else
      _ -> false
    end
  end

  defp nonzero(0), do: :error
  defp nonzero(target), do: {:ok, target}

  defp bounded(target) when target >= @two_256, do: :error
  defp bounded(target), do: {:ok, target}
end
