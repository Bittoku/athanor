defmodule Athanor.Indexer.ReconcileTest do
  @moduledoc """
  Phase 7 F7.2 (T7.2) — the pure RPC reconcile-by-hash core (ported from the
  review-confirmed MR !18 implementation at `5472b2b`).

    * `reconcile_plan/4` walks local vs node hashes to the common ancestor →
      `:synced | :defer | {:catch_up, from, to} | {:reorg, ancestor, to}`;
    * `branch_for/3` turns a plan + `node_hash_at` into an `apply_branch/2` arg
      (`{:apply, %{rollback_to, connect}}`) using the **contiguous** node-hash
      prefix, or `:synced`/`:defer`.

  `local_hash_at`/`node_hash_at` are `(height -> hash | nil)`. No IO.
  """
  use ExUnit.Case, async: true

  alias Athanor.Indexer.Reconcile

  describe "reconcile_plan/4" do
    test "empty local index → catch up from height 1" do
      assert Reconcile.reconcile_plan(0, 5, fn _ -> nil end, fn _ -> "ab" end) ==
               {:catch_up, 1, 5}
    end

    test "equal heights with a matching tip → synced" do
      same = fn _ -> "aa" end
      assert Reconcile.reconcile_plan(5, 5, same, same) == :synced
    end

    test "behind with a matching tip → forward catch-up" do
      local = fn h -> %{5 => "aa"}[h] end
      node = fn h -> %{5 => "aa", 6 => "bb", 7 => "cc"}[h] end
      assert Reconcile.reconcile_plan(5, 7, local, node) == {:catch_up, 6, 7}
    end

    test "equal heights but divergent tip → reorg to the common ancestor" do
      local = fn h -> %{3 => "c", 4 => "ld4", 5 => "ld5"}[h] end
      node = fn h -> %{3 => "c", 4 => "nd4", 5 => "nd5"}[h] end
      assert Reconcile.reconcile_plan(5, 5, local, node) == {:reorg, 3, 5}
    end

    test "orphaned local tip with the node ahead → reorg from the common ancestor" do
      local = fn h -> %{3 => "c", 4 => "ld4", 5 => "ld5"}[h] end
      node = fn h -> %{3 => "c", 4 => "nd4", 5 => "nd5", 6 => "nd6"}[h] end
      assert Reconcile.reconcile_plan(5, 6, local, node) == {:reorg, 3, 6}
    end

    test "an unknown hash below the tip → defer (never deep-rollback on an unproven ancestor)" do
      local = fn h -> %{5 => "ld5", 4 => "ld4"}[h] end
      node = fn h -> %{5 => "nd5"}[h] end
      assert Reconcile.reconcile_plan(5, 5, local, node) == :defer
    end
  end

  describe "branch_for/3" do
    test ":synced and :defer pass through" do
      assert Reconcile.branch_for(:synced, fn _ -> nil end, 10) == :synced
      assert Reconcile.branch_for(:defer, fn _ -> nil end, 10) == :defer
    end

    test "catch_up → apply with no rollback and the contiguous node-hash prefix" do
      node = fn h -> %{6 => "C6", 7 => "C7"}[h] end

      assert {:apply, %{rollback_to: nil, connect: connect}} =
               Reconcile.branch_for({:catch_up, 6, 7}, node, 10)

      assert connect == [Base.decode16!("C6"), Base.decode16!("C7")]
    end

    test "catch_up stops at the first gap (and defers if the first hash is missing)" do
      node = fn h -> %{6 => "C6", 8 => "C8"}[h] end
      assert {:apply, %{connect: [c6]}} = Reconcile.branch_for({:catch_up, 6, 8}, node, 10)
      assert c6 == Base.decode16!("C6")

      missing_first = fn h -> %{7 => "C7"}[h] end
      assert Reconcile.branch_for({:catch_up, 6, 7}, missing_first, 10) == :defer
    end

    test "reorg → apply with rollback to ancestor and the canonical branch from ancestor+1" do
      node = fn h -> %{4 => "C4", 5 => "C5"}[h] end

      assert {:apply, %{rollback_to: 3, connect: connect}} =
               Reconcile.branch_for({:reorg, 3, 5}, node, 10)

      assert connect == [Base.decode16!("C4"), Base.decode16!("C5")]
    end

    test "reorg with the node at/below the ancestor → rollback only (empty connect)" do
      assert {:apply, %{rollback_to: 3, connect: []}} =
               Reconcile.branch_for({:reorg, 3, 3}, fn _ -> nil end, 10)
    end

    test "reorg defers when the canonical block at ancestor+1 is unavailable" do
      node = fn h -> %{6 => "C6"}[h] end
      assert Reconcile.branch_for({:reorg, 3, 6}, node, 10) == :defer
    end

    test "the connect prefix is capped to `batch` blocks per cycle" do
      node = fn h -> %{6 => "C6", 7 => "C7", 8 => "C8"}[h] end
      assert {:apply, %{connect: connect}} = Reconcile.branch_for({:catch_up, 6, 8}, node, 2)
      assert length(connect) == 2
    end
  end
end
