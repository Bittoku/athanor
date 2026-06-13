defmodule Athanor.P2P.HeadersChain.TreeTest do
  @moduledoc """
  Tests for the pure headers tree `Athanor.P2P.HeadersChain.Tree` (Phase 6 T6.0,
  §A). `step(tree, event) -> {tree, events}` — no process, no IO. Tracks the best
  tip by **cumulative work** (never height), validates PoW per header (injectable
  `:pow_check` seam so the work/reorg logic is testable without mining), detects
  reorgs by the common-ancestor walk emitting orphan/connect sets, drops detached
  headers, and prunes to `root` + descendants.

  Byte order is wire/internal throughout: parent links use
  `BlockHeader.prev_hash_wire/1` (raw `prev_block`), node keys use
  `BlockHeader.hash/1`.
  """
  use ExUnit.Case, async: true

  alias Athanor.P2P.HeadersChain.{Tree, Work}
  alias Athanor.P2P.Messages.BlockHeader

  @easy 0x207FFFFF
  @hard 0x1D00FFFF

  defp mk(prev_wire, bits, nonce \\ 0) do
    raw =
      <<1::little-32>> <>
        prev_wire <> <<0::256>> <> <<0::little-32>> <> <<bits::little-32>> <> <<nonce::little-32>>

    %BlockHeader{raw: raw}
  end

  # Build a chain of headers (each prev = previous hash) from `start_wire`. `salt`
  # distinguishes sibling forks: two headers off the same parent with the same
  # bits but different salts get different hashes (a real fork, not a duplicate).
  defp chain(start_wire, bits_list, salt \\ 0) do
    {headers, _} =
      bits_list
      |> Enum.with_index()
      |> Enum.map_reduce(start_wire, fn {bits, i}, prev ->
        header = mk(prev, bits, salt * 1_000 + i)
        {header, BlockHeader.hash(header)}
      end)

    headers
  end

  defp h(header), do: BlockHeader.hash(header)
  defp seed_hash, do: :binary.copy(<<0x50>>, 32)
  defp seed_tree(opts), do: Tree.new(seed_hash(), 100, opts)

  # Default trees in tests bypass PoW (so we can set arbitrary bits/work); the
  # real PoW path is covered by its own test.
  defp open_tree(opts \\ []) do
    Tree.new(seed_hash(), 100, Keyword.merge([pow_check: fn _h, _b -> true end], opts))
  end

  describe "connect / extend" do
    test "a header whose raw prev_block matches a stored node connects (not detached)" do
      tree = open_tree()
      [h1] = chain(seed_hash(), [@easy])
      {tree, events} = Tree.step(tree, {:connect, [h1]})

      assert Map.has_key?(tree.nodes, h(h1)), "header must connect to the seed, not be detached"
      assert tree.nodes[h(h1)].prev == seed_hash()
      refute Enum.any?(events, &match?({:detached, _}, &1))
    end

    test "a PoW-valid linear run extends the tip; cum_work increases with height" do
      tree = open_tree()
      [h1, h2] = chain(seed_hash(), [@easy, @easy])
      {tree, events} = Tree.step(tree, {:connect, [h1, h2]})

      assert events == [{:extend, [h(h1), h(h2)]}]
      assert tree.tip == h(h2)
      assert tree.nodes[h(h2)].height == 102
      assert tree.nodes[h(h2)].cum_work > tree.nodes[seed_hash()].cum_work
    end
  end

  describe "cumulative work + reorg" do
    test "a higher-work SHORTER branch overtakes a longer lower-work branch (work, not height)" do
      tree = open_tree()
      # Branch A: three easy headers off the seed.
      [a1, a2, a3] = chain(seed_hash(), [@easy, @easy, @easy])
      {tree, _} = Tree.step(tree, {:connect, [a1, a2, a3]})
      assert tree.tip == h(a3)

      # Branch B: a single HARD header off the seed — far more cumulative work.
      [b1] = chain(seed_hash(), [@hard])
      {tree, events} = Tree.step(tree, {:connect, [b1]})

      assert tree.tip == h(b1), "tip must switch by work, not height"

      assert events == [
               {:reorg, %{orphan: [h(a3), h(a2), h(a1)], connect: [h(b1)]}}
             ]
    end

    test "a fork that does not out-work the tip is stored but does not switch the tip" do
      tree = open_tree()
      [a1, a2] = chain(seed_hash(), [@easy, @easy])
      {tree, _} = Tree.step(tree, {:connect, [a1, a2]})

      # A single easy header off the seed (distinct salt → a real sibling fork):
      # less cum_work than the 2-deep tip.
      [b1] = chain(seed_hash(), [@easy], 1)
      {tree, events} = Tree.step(tree, {:connect, [b1]})

      assert tree.tip == h(a2)
      assert Map.has_key?(tree.nodes, h(b1))
      assert events == []
    end
  end

  describe "PoW validation (real check)" do
    test "a header that does not meet its target is rejected (not added, no event)" do
      tree = seed_tree(pow_check: &Work.meets_target?/2)
      # Hard bits + an unmined header → hash (~2^256) far exceeds the target.
      [bad] = chain(seed_hash(), [@hard])
      refute Work.meets_target?(h(bad), @hard)

      {tree, events} = Tree.step(tree, {:connect, [bad]})
      refute Map.has_key?(tree.nodes, h(bad))
      assert tree.tip == seed_hash()
      assert events == []
    end
  end

  describe "detached" do
    test "a header with an unknown parent leaves the tree unchanged and yields {:detached, _}" do
      tree = open_tree()
      orphan = mk(:binary.copy(<<0x99>>, 32), @easy)
      {tree2, events} = Tree.step(tree, {:connect, [orphan]})

      refute Map.has_key?(tree2.nodes, h(orphan))
      assert tree2.nodes == tree.nodes
      assert events == [{:detached, 1}]
    end
  end

  describe "prune" do
    test "prunes below tip.height - window and advances root to the window low edge" do
      tree = open_tree(window: 3)
      headers = chain(seed_hash(), List.duplicate(@easy, 5))
      {tree, _} = Tree.step(tree, {:connect, headers})
      [h1, h2, h3, h4, h5] = Enum.map(headers, &h/1)
      assert tree.tip == h5

      {tree, _} = Tree.step(tree, :prune)
      # tip height 105, window 3 → cutoff 102 → new root is h2 (height 102).
      assert tree.root == h2
      assert tree.nodes[h2].prev == nil
      assert Map.keys(tree.nodes) |> Enum.sort() == Enum.sort([h2, h3, h4, h5])
      refute Map.has_key?(tree.nodes, seed_hash())
      refute Map.has_key?(tree.nodes, h1)
    end

    test "a fork whose fork point is pruned is dropped; its later extension is detached" do
      tree = open_tree(window: 2)
      main = chain(seed_hash(), List.duplicate(@easy, 5))
      [m1, _m2, _m3, _m4, _m5] = main
      # A distinct fork that is a child of m1 (height 102), salt 1.
      [f1] = chain(h(m1), [@easy], 1)
      {tree, _} = Tree.step(tree, {:connect, main ++ [f1]})
      assert Map.has_key?(tree.nodes, h(f1))

      {tree, _} = Tree.step(tree, :prune)
      # tip height 105, window 2 → cutoff 103 → f1 (102) is below and unrootable.
      refute Map.has_key?(tree.nodes, h(f1))

      # An extension of the pruned fork now has no known parent → detached.
      [f2] = chain(h(f1), [@easy], 2)
      {_tree, events} = Tree.step(tree, {:connect, [f2]})
      assert events == [{:detached, 1}]
    end
  end

  describe "locator" do
    test "short chain: tip back to root, no duplicates, root last" do
      tree = open_tree()
      headers = chain(seed_hash(), List.duplicate(@easy, 6))
      {tree, _} = Tree.step(tree, {:connect, headers})
      tip = List.last(Enum.map(headers, &h/1))

      {_tree, [{:locator, locator}]} = Tree.step(tree, {:locator, 32})
      assert hd(locator) == tip
      assert List.last(locator) == seed_hash()
      assert length(Enum.uniq(locator)) == length(locator)
    end

    test "deep chain: exponential step-back — far fewer hashes than blocks, widening gaps, root last" do
      tree = open_tree(window: 1_000)
      headers = chain(seed_hash(), List.duplicate(@easy, 40))
      {tree, _} = Tree.step(tree, {:connect, headers})
      tip = List.last(Enum.map(headers, &h/1))

      {_tree, [{:locator, locator}]} = Tree.step(tree, {:locator, 32})
      assert hd(locator) == tip
      assert List.last(locator) == seed_hash()
      # Exponential, not linear: 41 nodes collapse to far fewer locator entries.
      assert length(locator) < 20

      heights = Enum.map(locator, fn hash -> tree.nodes[hash].height end)
      # Strictly decreasing, and the back-steps widen beyond 1 (not a linear walk).
      assert heights == Enum.sort(heights, :desc)
      assert length(Enum.uniq(heights)) == length(heights)
      gaps = heights |> Enum.chunk_every(2, 1, :discard) |> Enum.map(fn [a, b] -> a - b end)
      assert Enum.any?(gaps, &(&1 > 1)), "expected exponential widening, got linear gaps"
    end

    test "enforces the max length including root" do
      tree = open_tree(window: 1_000)
      headers = chain(seed_hash(), List.duplicate(@easy, 40))
      {tree, _} = Tree.step(tree, {:connect, headers})

      {_tree, [{:locator, locator}]} = Tree.step(tree, {:locator, 5})
      assert length(locator) <= 5
      assert List.last(locator) == seed_hash()
    end
  end
end
