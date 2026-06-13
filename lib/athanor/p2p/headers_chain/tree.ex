defmodule Athanor.P2P.HeadersChain.Tree do
  @moduledoc """
  Pure headers-chain reducer (Phase 6 T6.0, §A). Maintains a bounded window of
  block headers learned over P2P, chooses the best tip by **cumulative work**
  (never height), validates each header's proof-of-work before crediting its work,
  detects reorgs by the common-ancestor walk, and prunes to `root` + descendants.
  No process, no IO, no DB.

  `step(tree, event) -> {tree, events}` where:

    * events — `{:connect, [%BlockHeader{}]}`, `{:locator, n}`, `:prune`.
    * emitted — `{:extend, [wire_hash]}`, `{:reorg, %{orphan: [...], connect: [...]}}`,
      `{:detached, count}`, `{:locator, [wire_hash]}`.

  ## State
  `nodes` maps a **wire-order** `BlockHeader.hash/1` to
  `%{header, height, work, cum_work, prev}`. The single **synthetic seed root**
  (planted by `new/3` from the REST tip) is the only node with `header: nil`,
  `prev: nil`, `work: 0`, `cum_work: 0`; real headers attach above it via
  `BlockHeader.prev_hash_wire/1` (the raw wire-order parent). `tip` is the node of
  greatest `cum_work` (ties by first-seen via an insertion `seq`); `root` is the
  window's low edge.

  ## Invariants
  Parent lookup and the reorg walk use wire order exclusively (display conversion
  is the store boundary's job). `:prune` retains **only `root` and its
  descendants**, so every retained node's `prev` chain reaches `root` and any two
  retained tips share an in-window common ancestor — the reducer therefore always
  emits a well-formed `{:reorg, …}` and never a partial set. A fork whose fork
  point is pruned is dropped; its later extension re-enters as `{:detached, …}`
  (the GenServer escalates persistent detachment to the deep-reorg fallback).

  PoW is checked through an injected `:pow_check` (`(wire_hash, bits) -> boolean`,
  default `Work.meets_target?/2`) so the work/reorg logic is unit-testable without
  mining; a header failing the check is rejected (never added, no work credited).
  """

  alias Athanor.P2P.HeadersChain.Work
  alias Athanor.P2P.Messages.BlockHeader

  defstruct nodes: %{}, tip: nil, root: nil, window: 144, seq: 0, pow_check: nil

  @type wire_hash :: <<_::256>>
  @type t :: %__MODULE__{}

  @doc """
  Builds a tree seeded with a synthetic root at `seed_hash` (wire order) /
  `seed_height`. Options: `:window` (default 144), `:pow_check` (default
  `&Work.meets_target?/2`).
  """
  @spec new(wire_hash(), non_neg_integer(), keyword()) :: t()
  def new(seed_hash, seed_height, opts \\ []) do
    root = %{header: nil, height: seed_height, work: 0, cum_work: 0, prev: nil, seq: 0}

    %__MODULE__{
      nodes: %{seed_hash => root},
      tip: seed_hash,
      root: seed_hash,
      window: Keyword.get(opts, :window, 144),
      seq: 1,
      pow_check: Keyword.get(opts, :pow_check, &Work.meets_target?/2)
    }
  end

  @doc "The height of the window's low edge (the synthetic root / seed height)."
  @spec root_height(t()) :: non_neg_integer()
  def root_height(%__MODULE__{root: root, nodes: nodes}), do: nodes[root].height

  @doc "Advances the tree by one event. See the module doc."
  @spec step(t(), {:connect, [BlockHeader.t()]} | {:locator, pos_integer()} | :tick | :prune) ::
          {t(), [tuple()]}
  def step(%__MODULE__{} = tree, {:connect, headers}) do
    old_tip = tree.tip
    {tree, detached} = Enum.reduce(headers, {tree, 0}, &connect_one/2)

    events =
      if detached > 0, do: [{:detached, detached}], else: []

    case best_tip(tree) do
      ^old_tip -> {%{tree | tip: old_tip}, events}
      new_tip -> {%{tree | tip: new_tip}, [tip_event(tree, old_tip, new_tip) | events]}
    end
  end

  def step(%__MODULE__{} = tree, {:locator, n}) do
    {tree, [{:locator, locator(tree, n)}]}
  end

  def step(%__MODULE__{} = tree, :prune), do: {prune(tree), []}

  # `:tick` is a no-op at the reducer level (the GenServer schedules prune/getheaders).
  def step(%__MODULE__{} = tree, :tick), do: {tree, []}

  ## ── connect ──

  defp connect_one(header, {tree, detached}) do
    hash = BlockHeader.hash(header)
    parent = BlockHeader.prev_hash_wire(header)

    cond do
      Map.has_key?(tree.nodes, hash) -> {tree, detached}
      not Map.has_key?(tree.nodes, parent) -> {tree, detached + 1}
      not tree.pow_check.(hash, BlockHeader.bits(header)) -> {tree, detached}
      true -> {add_node(tree, hash, parent, header), detached}
    end
  end

  defp add_node(tree, hash, parent, header) do
    {:ok, work} = Work.work(BlockHeader.bits(header))
    parent_node = tree.nodes[parent]

    node = %{
      header: header,
      height: parent_node.height + 1,
      work: work,
      cum_work: parent_node.cum_work + work,
      prev: parent,
      seq: tree.seq
    }

    %{tree | nodes: Map.put(tree.nodes, hash, node), seq: tree.seq + 1}
  end

  # The node of greatest cum_work; ties broken by lowest seq (first-seen).
  defp best_tip(tree) do
    {hash, _} =
      Enum.max_by(tree.nodes, fn {_h, n} -> {n.cum_work, -n.seq} end)

    hash
  end

  # The net tip move from `old` to `new`: an extend if `new` descends from `old`,
  # else a reorg with the common-ancestor split.
  defp tip_event(tree, old, new) do
    if descendant?(tree, new, old) do
      {:extend, path_between(tree, new, old)}
    else
      ancestor = common_ancestor(tree, old, new)

      {:reorg,
       %{orphan: path_down_to(tree, old, ancestor), connect: path_between(tree, new, ancestor)}}
    end
  end

  ## ── ancestry walks (wire order via `prev`) ──

  defp descendant?(_tree, hash, hash), do: true

  defp descendant?(tree, hash, ancestor) do
    case tree.nodes[hash] do
      %{prev: nil} -> false
      %{prev: prev} -> descendant?(tree, prev, ancestor)
      nil -> false
    end
  end

  # Hashes from `to_ancestor` (exclusive) up to `from` — i.e. the new blocks in
  # ancestor→descendant (height) order.
  defp path_between(tree, from, to_ancestor) do
    from |> ancestors_until(tree, to_ancestor) |> Enum.reverse()
  end

  # Hashes from `from` down to `to_ancestor` (exclusive) — descendant→ancestor.
  defp path_down_to(tree, from, to_ancestor), do: ancestors_until(from, tree, to_ancestor)

  defp ancestors_until(hash, tree, stop), do: do_ancestors(tree, hash, stop, [])

  defp do_ancestors(_tree, stop, stop, acc), do: Enum.reverse(acc)

  defp do_ancestors(tree, hash, stop, acc) do
    case tree.nodes[hash] do
      %{prev: prev} -> do_ancestors(tree, prev, stop, [hash | acc])
      nil -> Enum.reverse(acc)
    end
  end

  defp common_ancestor(tree, a, b) do
    a_chain = chain_set(tree, a)
    walk_to_shared(tree, b, a_chain)
  end

  defp chain_set(tree, hash), do: chain_set(tree, hash, MapSet.new())
  defp chain_set(_tree, nil, set), do: set

  defp chain_set(tree, hash, set) do
    case tree.nodes[hash] do
      %{prev: prev} -> chain_set(tree, prev, MapSet.put(set, hash))
      nil -> set
    end
  end

  defp walk_to_shared(tree, hash, shared) do
    cond do
      MapSet.member?(shared, hash) -> hash
      true -> walk_to_shared(tree, tree.nodes[hash].prev, shared)
    end
  end

  ## ── locator ──

  # Bitcoin-style block locator: the tip, then step back by 1 for the first ten
  # hashes and **double the stride** thereafter (1,1,…,2,4,8,…), always ending at
  # `root`, capped to `n` hashes total (including root). The exponential step-back
  # lets a peer find the fork point in O(log height) even for a deep chain.
  defp locator(tree, n) do
    collect_locator(tree, tree.tip, 1, 0, [], n)
    |> Enum.reverse()
    |> ensure_root_last(tree.root)
    |> cap_with_root(n)
  end

  # `acc` accumulates oldest-first (each visited hash is prepended); `stride`
  # doubles once `count` (hashes collected) reaches 10.
  defp collect_locator(_tree, nil, _stride, _count, acc, _n), do: acc
  defp collect_locator(_tree, _hash, _stride, _count, acc, n) when length(acc) >= n, do: acc

  defp collect_locator(tree, hash, stride, count, acc, n) do
    acc = [hash | acc]
    stride = if count >= 10, do: stride * 2, else: stride
    collect_locator(tree, step_back(tree, hash, stride), stride, count + 1, acc, n)
  end

  # Walk back exactly `k` parents; `nil` once the walk passes the root (`prev: nil`).
  defp step_back(_tree, hash, 0), do: hash

  defp step_back(tree, hash, k) do
    case tree.nodes[hash] do
      %{prev: nil} -> nil
      %{prev: prev} -> step_back(tree, prev, k - 1)
      nil -> nil
    end
  end

  defp ensure_root_last(hashes, root) do
    if List.last(hashes) == root, do: hashes, else: hashes ++ [root]
  end

  # Enforce the max length `n` while keeping `root` as the final entry — drop from
  # the dense newest hashes, never the root.
  defp cap_with_root(hashes, n) when length(hashes) <= n, do: hashes

  defp cap_with_root(hashes, n) do
    Enum.take(hashes, n - 1) ++ [List.last(hashes)]
  end

  ## ── prune ──

  defp prune(%__MODULE__{} = tree) do
    cutoff = tree.nodes[tree.tip].height - tree.window

    if cutoff <= tree.nodes[tree.root].height do
      tree
    else
      new_root = active_node_at_height(tree, tree.tip, cutoff)
      kept = reachable_from(tree, new_root)

      nodes =
        tree.nodes
        |> Map.take(MapSet.to_list(kept))
        |> Map.update!(new_root, &%{&1 | prev: nil})

      %{tree | nodes: nodes, root: new_root}
    end
  end

  defp active_node_at_height(tree, hash, target_height) do
    node = tree.nodes[hash]

    if node.height == target_height,
      do: hash,
      else: active_node_at_height(tree, node.prev, target_height)
  end

  # The set of nodes whose `prev` chain reaches `root_hash` (root + descendants).
  defp reachable_from(tree, root_hash) do
    for {hash, _node} <- tree.nodes, reaches?(tree, hash, root_hash), into: MapSet.new(), do: hash
  end

  defp reaches?(_tree, hash, hash), do: true

  defp reaches?(tree, hash, root) do
    case tree.nodes[hash] do
      %{prev: nil} -> false
      %{prev: prev} -> reaches?(tree, prev, root)
      nil -> false
    end
  end
end
