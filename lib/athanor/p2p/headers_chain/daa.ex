defmodule Athanor.P2P.HeadersChain.Daa do
  @moduledoc """
  Phase 7 F7.1 (§3) — the pure, security-critical **BSV cw-144 difficulty
  adjustment** core. Given a candidate header's in-window parent chain, it
  recomputes the consensus-required compact `nBits` so the headers tree can reject
  an *easier-than-consensus* header before crediting its work
  (`docs/thin-node-p2p-phase7-f71-daa.md`).

  BSV inherited the Bitcoin-Cash Nov-2017 DAA ("cw-144"): every block retargets
  from a rolling 144-block window. This module mirrors bitcoin-abc's
  `GetNextCashWorkRequired` / `GetSuitableBlock` / `ComputeTarget` exactly (pinned
  by mainnet golden vectors in the test suite).

  ## Node shape
  The functions operate on lightweight **DAA nodes** — any map exposing
  `:time` (the header `nTime`, a `uint32`) and `:cum_work` (cumulative chain work
  through that block, the same quantity the headers `Tree` tracks). Ancestor
  navigation is via an injected `ancestor_fun` so this module stays pure and
  decoupled from the tree's storage (`HeadersChain.Tree` supplies it; the synthetic
  `header: nil` root is never a DAA node — the bootstrap seeds a full real window,
  see §4.2 / §D1 of the design doc).

  ## Algorithm
    * `last  = suitable(P)`        — median-by-time of `{P, P-1, P-2}`
    * `first = suitable(P-144)`    — median-by-time of `{P-144, P-145, P-146}`
      (so the deepest ancestor read is `P-146`).
    * `work      = (last.cum_work - first.cum_work) * 600`  (600s target spacing)
    * `timespan  = clamp(last.time - first.time, 72*600, 288*600)`
    * `projected = work div timespan`
    * `next      = (2^256 - projected) div projected`  (= floor(2^256/projected) - 1)
    * `next      = min(next, pow_limit_target)`  (never easier than the pow-limit)
    * result `= Work.target_to_compact(next)` — the single canonical compact.

  Pure (no IO, no clock, no network).
  """

  alias Athanor.P2P.HeadersChain.Work

  @two_256 Integer.pow(2, 256)
  @target_spacing 600
  @daa_interval 144

  @type daa_node :: %{
          required(:time) => non_neg_integer(),
          required(:cum_work) => non_neg_integer()
        }
  @type ancestor_fun :: (any(), non_neg_integer() -> any() | nil)

  @doc """
  `GetSuitableBlock`: the median-by-`:time` of a 3-node triple, given in
  **consensus `[B, B-1, B-2]` order** (anchor first), via the same 3-element
  sorting network bitcoin-abc uses — slots `(0,2),(0,1),(1,2)`, swapping only on a
  strict `>`, returning slot 1. This de-noises a retarget endpoint against a single
  skewed timestamp. The input order is consensus-relevant for **equal timestamps**:
  the stable, swap-on-`>`-only network returns a different element of an equal-time
  triple depending on input order, and that element's `:cum_work` feeds the
  retarget — so the triple must be `[B, B-1, B-2]`, not the reverse.
  """
  @spec suitable([daa_node()]) :: daa_node()
  def suitable([b0, b1, b2]) do
    # 3-element sorting network by :time; the median lands in slot 1. Mirrors
    # bitcoin-abc GetSuitableBlock on the ordered triple {B, B-1, B-2}.
    {b0, b2} = swap_by_time(b0, b2)
    {b0, b1} = swap_by_time(b0, b1)
    {b1, _b2} = swap_by_time(b1, b2)
    _ = b0
    b1
  end

  @doc """
  `ComputeTarget`: the cw-144 retarget from the two suitable endpoints and the
  network pow-limit *target*, returning the canonical compact `nBits`.
  """
  @spec compute_target(daa_node(), daa_node(), pos_integer()) :: Work.compact()
  def compute_target(first, last, pow_limit_target) do
    work = (last.cum_work - first.cum_work) * @target_spacing
    timespan = clamp(last.time - first.time, 72 * @target_spacing, 288 * @target_spacing)
    projected = div(work, timespan)
    next = div(@two_256 - projected, projected)
    next = min(next, pow_limit_target)
    Work.target_to_compact(next)
  end

  @doc """
  Computes the consensus-required compact `nBits` for the child of `parent_node`
  (`P`). `ancestor_fun.(node, n)` returns the `n`-th ancestor of `node` (n = 0 →
  `node`) or `nil` when the walk leaves the retained real-header window.
  `pow_limit_compact` is the network pow-limit `nBits` (mainnet/testnet
  `0x1d00ffff`).

  ## Returns
    * `{:ok, canonical_compact}` — compare a candidate's **raw** `bits` to this
      exactly (a non-canonical encoding fails).
    * `{:error, :insufficient_window}` — a needed ancestor (`P..P-146`) is missing;
      the caller fails closed (it must not occur in normal operation given the
      seeded bootstrap window).
  """
  @spec expected_bits(daa_node(), ancestor_fun(), Work.compact()) ::
          {:ok, Work.compact()} | {:error, :insufficient_window}
  def expected_bits(parent_node, ancestor_fun, pow_limit_compact) do
    with last_triple when is_list(last_triple) <- window(parent_node, ancestor_fun, 0),
         first_triple when is_list(first_triple) <-
           window(parent_node, ancestor_fun, @daa_interval),
         {:ok, pow_limit_target} <- Work.compact_to_target(pow_limit_compact) do
      last = suitable(last_triple)
      first = suitable(first_triple)
      {:ok, compute_target(first, last, pow_limit_target)}
    else
      _ -> {:error, :insufficient_window}
    end
  end

  # The triple `[anchor, anchor-1, anchor-2]` (consensus `{B, B-1, B-2}` order)
  # where anchor = P-`offset`, or `nil` if any of the three ancestors is outside the
  # retained real-header window. The anchor-first order is required so `suitable/1`
  # reproduces bitcoin-abc `GetSuitableBlock` tie semantics for equal timestamps.
  defp window(parent_node, ancestor_fun, offset) do
    with anchor when not is_nil(anchor) <- ancestor_fun.(parent_node, offset),
         anchor_1 when not is_nil(anchor_1) <- ancestor_fun.(parent_node, offset + 1),
         anchor_2 when not is_nil(anchor_2) <- ancestor_fun.(parent_node, offset + 2) do
      [anchor, anchor_1, anchor_2]
    else
      _ -> nil
    end
  end

  defp swap_by_time(x, y), do: if(x.time > y.time, do: {y, x}, else: {x, y})

  defp clamp(v, lo, _hi) when v < lo, do: lo
  defp clamp(v, _lo, hi) when v > hi, do: hi
  defp clamp(v, _lo, _hi), do: v
end
