defmodule Athanor.Indexer.Reconcile do
  @moduledoc """
  Phase 7 F7.2 (T7.2) — the **pure** RPC reconcile-by-hash core, ported from the
  review-confirmed MR !18 implementation (`5472b2b`). It plans the index's recovery
  toward the RPC node tip by **hash** (never height), and turns that plan into a
  `BlockProcessor.apply_branch/2` argument using only a **contiguous** node-hash
  prefix — so the index is never recorded over a gap. No IO; the owning
  `TipController` supplies the `(height -> hash | nil)` seams and dispatches.

  ## `reconcile_plan/4`
  Walk down from `min(local_height, node_height)` to the highest height where the
  local and node hashes are **both known and equal** (the common ancestor):

    * `:synced` — heights equal and tips agree;
    * `{:catch_up, from, to}` — no divergence, the node is simply ahead;
    * `{:reorg, ancestor, to}` — the chains diverge: roll back to `ancestor` and
      reprocess the canonical branch from `ancestor + 1` up to `to`;
    * `:defer` — a hash below the tip is **unknown**, so the ancestor cannot be
      positively proven this cycle (a transient/pruned hash must not look like a
      real mismatch and trigger a destructive deep rollback).

  ## `branch_for/3`
  Turns a plan + `node_hash_at` + `batch` into `:synced | :defer |
  {:apply, %{rollback_to: height | nil, connect: [hash_binary]}}`, using the
  contiguous prefix from the start height (capped to `batch`), deferring if the
  first required block is unavailable rather than recording a gap.
  """

  @type hash_at :: (non_neg_integer() -> binary() | nil)
  @type plan ::
          :synced
          | :defer
          | {:catch_up, pos_integer(), non_neg_integer()}
          | {:reorg, non_neg_integer(), non_neg_integer()}

  @spec reconcile_plan(non_neg_integer(), non_neg_integer(), hash_at(), hash_at()) :: plan()
  def reconcile_plan(0, node_height, _local_hash_at, _node_hash_at),
    do: {:catch_up, 1, node_height}

  def reconcile_plan(local_height, node_height, local_hash_at, node_hash_at) do
    case find_ancestor(min(local_height, node_height), local_hash_at, node_hash_at) do
      :defer ->
        :defer

      {:ok, ancestor} ->
        cond do
          ancestor == local_height and local_height == node_height -> :synced
          ancestor == local_height -> {:catch_up, local_height + 1, node_height}
          true -> {:reorg, ancestor, node_height}
        end
    end
  end

  # Highest height (≤ `h`) at which the local and node hashes are both known and
  # equal. Returns `:defer` the moment either hash is unknown before a match is
  # found — an unknown hash is NOT a mismatch, so we never walk past it into a
  # destructive deep rollback without positively proving the ancestor.
  defp find_ancestor(h, _local_hash_at, _node_hash_at) when h < 0, do: :defer

  defp find_ancestor(h, local_hash_at, node_hash_at) do
    lh = local_hash_at.(h)
    nh = node_hash_at.(h)

    cond do
      is_nil(lh) or is_nil(nh) -> :defer
      lh == nh -> {:ok, h}
      true -> find_ancestor(h - 1, local_hash_at, node_hash_at)
    end
  end

  @doc """
  Builds the `apply_branch/2` argument for a plan (see the module doc). Returns
  `:synced`, `:defer`, or `{:apply, %{rollback_to: height | nil, connect: [hash]}}`.
  """
  @spec branch_for(plan(), hash_at(), pos_integer()) ::
          :synced
          | :defer
          | {:apply, %{rollback_to: non_neg_integer() | nil, connect: [binary()]}}
  def branch_for(plan, node_hash_at, batch \\ 10)

  def branch_for(:synced, _node_hash_at, _batch), do: :synced
  def branch_for(:defer, _node_hash_at, _batch), do: :defer

  def branch_for({:catch_up, from, to}, node_hash_at, batch) do
    case branch_hashes(from, to, node_hash_at, batch) do
      # The first canonical block is unavailable — defer rather than record a gap.
      [] -> :defer
      hashes -> {:apply, %{rollback_to: nil, connect: hashes}}
    end
  end

  # `to <= ancestor`: the node is at/below the common ancestor — the local tip is an
  # orphan fork above it. Roll back to the ancestor; there is no canonical branch to
  # connect.
  def branch_for({:reorg, ancestor, to}, _node_hash_at, _batch) when to <= ancestor,
    do: {:apply, %{rollback_to: ancestor, connect: []}}

  def branch_for({:reorg, ancestor, to}, node_hash_at, batch) do
    # Require the canonical block at `ancestor + 1` to be available (a contiguous
    # prefix) before rolling back, so we never roll back into a gap.
    case branch_hashes(ancestor + 1, to, node_hash_at, batch) do
      [] -> :defer
      connect -> {:apply, %{rollback_to: ancestor, connect: connect}}
    end
  end

  # The contiguous decoded node block-hash prefix for `from..to`, capped to `batch`
  # blocks per cycle. Stops at the first missing/invalid hash (returning the prefix
  # only) so a gap is never silently bridged — the remainder is picked up by later
  # cycles once the node has it.
  defp branch_hashes(from, to, _node_hash_at, _batch) when from > to, do: []

  defp branch_hashes(from, to, node_hash_at, batch) do
    contiguous_hashes(from, min(from + batch - 1, to), node_hash_at, [])
  end

  defp contiguous_hashes(height, last, _node_hash_at, acc) when height > last,
    do: Enum.reverse(acc)

  defp contiguous_hashes(height, last, node_hash_at, acc) do
    case decode_hash(node_hash_at.(height)) do
      nil -> Enum.reverse(acc)
      hash -> contiguous_hashes(height + 1, last, node_hash_at, [hash | acc])
    end
  end

  defp decode_hash(nil), do: nil

  defp decode_hash(hex) do
    case Base.decode16(hex, case: :mixed) do
      {:ok, binary} -> binary
      :error -> nil
    end
  end
end
