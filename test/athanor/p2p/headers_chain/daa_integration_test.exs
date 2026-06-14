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
end
