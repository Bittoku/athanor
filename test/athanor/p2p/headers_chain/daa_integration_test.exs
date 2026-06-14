defmodule Athanor.P2P.HeadersChain.DaaIntegrationTest do
  @moduledoc """
  Phase 7 F7.1 T7.1.5 — the cw-144 `daa_check` seam wired into `HeadersChain.Tree`.

  The window-building headers themselves cannot pass the DAA gate (they lack a full
  147-ancestor window — exactly the §D1 case the bootstrap seeds), so the test
  *seeds* them with the default `:ok` bypass, then arms the real
  `HeadersChain.daa_checker/1` and validates only **new** headers above the seeded
  window. A constant-difficulty, 600s-spaced window has a stable retarget, so the
  consensus-correct `bits` for the next block is the same constant `B`.
  """
  use ExUnit.Case, async: true

  alias Athanor.P2P.HeadersChain
  alias Athanor.P2P.HeadersChain.{Tree, Work}
  alias Athanor.P2P.Messages.BlockHeader

  @pow_limit 0x1D00FFFF
  # Stable under a constant-bits / 600s window; expected_bits(child) == @stable_bits.
  @stable_bits 0x1B010000
  # A non-canonical compact decoding to the SAME target as @stable_bits.
  @noncanonical_same_target 0x1C000100
  # Easier than @stable_bits (the pow-limit) — must be rejected as below-consensus.
  @easier_bits 0x1D00FFFF

  defp hdr(prev_wire, time, bits) do
    raw =
      <<1::little-32>> <>
        prev_wire <>
        :binary.copy(<<0x22>>, 32) <>
        <<time::little-32>> <>
        <<bits::little-32>> <>
        <<0::little-32>>

    %BlockHeader{raw: raw}
  end

  # `n` consecutive headers above `seed_hash`, constant `bits`, +600s each.
  defp build_window(seed_hash, seed_time, n, bits) do
    Enum.map_reduce(1..n, {seed_hash, seed_time}, fn _i, {prev, t} ->
      t = t + 600
      h = hdr(prev, t, bits)
      {h, {BlockHeader.hash(h), t}}
    end)
  end

  defp seeded_tree do
    seed_hash = :binary.copy(<<0xAA>>, 32)
    seed_height = 600_000
    seed_time = 1_600_000_000

    {window, {top_hash, top_time}} = build_window(seed_hash, seed_time, 147, @stable_bits)

    # Seed the window with the bypass (simulates the bootstrap-seeded real window),
    # then arm the real DAA gate for subsequent P2P-learned headers.
    {seeded, _} =
      Tree.new(seed_hash, seed_height, pow_check: always_ok())
      |> Tree.step({:connect, window})

    tree = %{seeded | daa_check: HeadersChain.daa_checker(@pow_limit)}
    %{tree: tree, top_hash: top_hash, top_time: top_time}
  end

  defp always_ok, do: fn _hash, _bits -> true end

  test "a consensus-correct new header connects and becomes the tip (b)" do
    %{tree: tree, top_hash: top, top_time: t} = seeded_tree()
    cand = hdr(top, t + 600, @stable_bits)
    {t1, _} = Tree.step(tree, {:connect, [cand]})

    assert Map.has_key?(t1.nodes, BlockHeader.hash(cand))
    assert t1.tip == BlockHeader.hash(cand)
  end

  test "an easier-than-consensus header is dropped and cannot become tip (a, acceptance)" do
    %{tree: tree, top_hash: top, top_time: t} = seeded_tree()
    cand = hdr(top, t + 600, @easier_bits)
    {t1, _} = Tree.step(tree, {:connect, [cand]})

    refute Map.has_key?(t1.nodes, BlockHeader.hash(cand))
    assert t1.tip == tree.tip
  end

  test "a non-canonical compact for the right target is rejected (c, I1)" do
    %{tree: tree, top_hash: top, top_time: t} = seeded_tree()
    # Same target as the (accepted) @stable_bits, but a non-canonical encoding.
    {:ok, target} = Work.compact_to_target(@noncanonical_same_target)
    {:ok, ^target} = Work.compact_to_target(@stable_bits)
    refute @noncanonical_same_target == @stable_bits

    cand = hdr(top, t + 600, @noncanonical_same_target)
    {t1, _} = Tree.step(tree, {:connect, [cand]})
    refute Map.has_key?(t1.nodes, BlockHeader.hash(cand))
  end

  test "insufficient window fails closed (d) — a header just above the synthetic seed is rejected" do
    # A fresh seed with NO seeded prefix; the first header has no DAA window.
    seed_hash = :binary.copy(<<0xBB>>, 32)

    tree =
      Tree.new(seed_hash, 700_000,
        pow_check: always_ok(),
        daa_check: HeadersChain.daa_checker(@pow_limit)
      )

    cand = hdr(seed_hash, 1_700_000_600, @stable_bits)
    {t1, _} = Tree.step(tree, {:connect, [cand]})
    refute Map.has_key?(t1.nodes, BlockHeader.hash(cand))
  end

  test "the seam passes a working ancestor_fun and rejects on any non-:ok result" do
    seed_hash = :binary.copy(<<0xCC>>, 32)
    test_pid = self()

    spy =
      fn parent_node, _header, ancestor_fun ->
        send(test_pid, {:daa_called, parent_node.height, ancestor_fun.(parent_node, 0)})
        {:error, :nope}
      end

    tree = Tree.new(seed_hash, 800_000, pow_check: always_ok(), daa_check: spy)
    cand = hdr(seed_hash, 1, @stable_bits)
    {t1, _} = Tree.step(tree, {:connect, [cand]})

    # parent is the synthetic seed (height 800_000); ancestor_fun.(seed, 0) is nil
    # because the seed has header: nil (not a DAA node).
    assert_received {:daa_called, 800_000, nil}
    refute Map.has_key?(t1.nodes, BlockHeader.hash(cand))
  end

  describe "retained window preserves the DAA window across prune (T7.1.6 / D2)" do
    test "after prune the full P..P-146 window survives and a new header still validates" do
      seed_hash = :binary.copy(<<0xDD>>, 32)
      {window, {top, top_time}} = build_window(seed_hash, 1_500_000_000, 300, @stable_bits)

      {seeded, _} =
        Tree.new(seed_hash, 500_000, pow_check: always_ok()) |> Tree.step({:connect, window})

      {pruned, _} = Tree.step(seeded, :prune)

      # The DAA window survived the prune: P..P-146 are all real ancestors.
      assert Tree.ancestor_fun(pruned).(pruned.nodes[top], 146) != nil

      # ...and the next consensus-correct header connects through the real gate.
      tree = %{pruned | daa_check: HeadersChain.daa_checker(@pow_limit)}
      cand = hdr(top, top_time + 600, @stable_bits)
      {t1, _} = Tree.step(tree, {:connect, [cand]})
      assert Map.has_key?(t1.nodes, BlockHeader.hash(cand))
    end

    test "a too-small retained window underflows and fails closed (never pow-only)" do
      seed_hash = :binary.copy(<<0xEE>>, 32)
      {window, {top, top_time}} = build_window(seed_hash, 1_500_000_000, 160, @stable_bits)

      {seeded, _} =
        Tree.new(seed_hash, 500_000, window: 5, pow_check: always_ok())
        |> Tree.step({:connect, window})

      {pruned, _} = Tree.step(seeded, :prune)
      tree = %{pruned | daa_check: HeadersChain.daa_checker(@pow_limit)}

      cand = hdr(top, top_time + 600, @stable_bits)
      {t1, _} = Tree.step(tree, {:connect, [cand]})
      refute Map.has_key?(t1.nodes, BlockHeader.hash(cand))
    end
  end

  describe "default_daa_check/2 (T7.1.7 network-resolved default, D3)" do
    test "the mainnet default is the real cw-144 checker (:ok for correct bits, error otherwise)" do
      %{tree: tree, top_hash: top} = seeded_tree()
      checker = HeadersChain.default_daa_check(:mainnet, @pow_limit)
      af = Tree.ancestor_fun(tree)
      parent = tree.nodes[top]

      assert checker.(parent, hdr(top, 1, @stable_bits), af) == :ok

      assert match?(
               {:error, :difficulty_mismatch},
               checker.(parent, hdr(top, 1, @easier_bits), af)
             )
    end

    test "the testnet default is a fail-closed stub — never a silent :ok" do
      stub = HeadersChain.default_daa_check(:testnet, @pow_limit)

      assert stub.(%{}, hdr(:binary.copy(<<0>>, 32), 1, @stable_bits), fn _n, _k -> nil end) ==
               {:error, :testnet_daa_unsupported}
    end
  end

  describe "bootstrap window seeding (T7.1.8 / D1)" do
    test "seed_window plants the real window above the synthetic root; the first header above the checkpoint DAA-validates" do
      root_hash = :binary.copy(<<0x11>>, 32)
      root_height = 499_853

      {window, {checkpoint, checkpoint_time}} =
        build_window(root_hash, 1_600_000_000, 147, @stable_bits)

      tree =
        Tree.new(root_hash, root_height,
          pow_check: always_ok(),
          daa_check: HeadersChain.daa_checker(@pow_limit)
        )
        |> Tree.seed_window(window)

      # The synthetic root is unchanged (header: nil) at seed-147; the checkpoint is
      # the topmost seeded real node and the tip.
      assert tree.root == root_hash
      assert tree.nodes[root_hash].header == nil
      assert tree.tip == checkpoint
      assert tree.nodes[checkpoint].height == root_height + 147

      # The first P2P header above the checkpoint is DAA-validated (no pow-only
      # boundary, I3) because it has a full real P..P-146 window.
      cand = hdr(checkpoint, checkpoint_time + 600, @stable_bits)
      {t1, _} = Tree.step(tree, {:connect, [cand]})
      assert Map.has_key?(t1.nodes, BlockHeader.hash(cand))

      # The synthetic root is never reached as a DAA node.
      assert Tree.ancestor_fun(t1).(tree.nodes[checkpoint], 147) == nil
    end

    test "seed_window raises on a discontiguous window (the caller seeds inert)" do
      root_hash = :binary.copy(<<0x22>>, 32)
      tree = Tree.new(root_hash, 100)
      orphan = hdr(:binary.copy(<<0x99>>, 32), 1, @stable_bits)

      assert_raise MatchError, fn -> Tree.seed_window(tree, [orphan]) end
    end
  end

  describe "ensure_seeded seed contract under the armed gate (T7.1.8 / D1, blocker 3)" do
    # A real HeadersChain GenServer whose `:seed` returns `seed_result`. With no
    # explicit `:daa_check` the production cw-144 gate is armed (the default mainnet
    # checker), so a windowless seed must leave the chain inert rather than build a
    # tree that rejects every header for an insufficient window.
    defp start_chain(seed_result, extra \\ []) do
      opts =
        [
          seed: fn -> seed_result end,
          pow_check: always_ok(),
          tick_interval_ms: 600_000,
          registry: :"reg_#{System.unique_integer([:positive])}"
        ] ++ extra

      start_supervised!({HeadersChain, opts}, id: {HeadersChain, make_ref()})
    end

    test "an armed chain refuses a windowless 3-tuple seed and stays inert" do
      pid = start_chain({:ok, 700_000, :binary.copy(<<0x31>>, 32)})
      assert GenServer.call(pid, :root_height) == nil
    end

    test "an armed chain accepts a 4-tuple seed and plants the real window" do
      seed_hash = :binary.copy(<<0x32>>, 32)
      {window, _} = build_window(seed_hash, 1_600_000_000, 147, @stable_bits)
      pid = start_chain({:ok, 600_000, seed_hash, window})

      assert GenServer.call(pid, :root_height) == 600_000
    end

    test "an explicit :daa_check bypass still honours the legacy 3-tuple seed" do
      pid =
        start_chain({:ok, 700_000, :binary.copy(<<0x33>>, 32)},
          daa_check: fn _p, _h, _a -> :ok end
        )

      assert GenServer.call(pid, :root_height) == 700_000
    end
  end
end
