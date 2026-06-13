defmodule Athanor.P2P.HeadersChain.WorkTest do
  @moduledoc """
  Tests for `Athanor.P2P.HeadersChain.Work` (Phase 6 T6.1, §A) — the pure,
  security-critical numeric core: decode a header's compact `bits` (nBits) to a
  256-bit `target`, derive the per-header proof-of-work `work = floor(2^256 /
  (target + 1))`, and check a block hash meets its target. A malformed (negative
  sign-bit / zero / overflowing) compact value is rejected so it can never credit
  bogus work.
  """
  use ExUnit.Case, async: true

  alias Athanor.P2P.HeadersChain.Work

  test "genesis bits 0x1d00ffff decode to the known target and work" do
    assert {:ok, target} = Work.compact_to_target(0x1D00FFFF)
    assert target == 0xFFFF * Integer.pow(2, 208)
    # Work of one min-difficulty block is the well-known 0x100010001.
    assert {:ok, 0x100010001} = Work.work(0x1D00FFFF)
  end

  test "a smaller target (harder difficulty) yields more work" do
    {:ok, easy} = Work.work(0x1D00FFFF)
    {:ok, hard} = Work.work(0x1C00FFFF)
    assert hard > easy
  end

  test "rejects a negative (sign-bit) mantissa" do
    assert :error = Work.compact_to_target(0x01800000)
    assert :error = Work.work(0x01800000)
  end

  test "rejects a zero target (zero mantissa)" do
    assert :error = Work.compact_to_target(0x00000000)
    assert :error = Work.compact_to_target(0x01000000)
  end

  test "rejects an overflowing (>= 2^256) target" do
    assert :error = Work.compact_to_target(0xFF7FFFFF)
  end

  test "meets_target?: a hash below target passes, above fails" do
    # The block hash is wire/internal order, interpreted little-endian.
    assert Work.meets_target?(:binary.copy(<<0>>, 32), 0x1D00FFFF)
    refute Work.meets_target?(:binary.copy(<<0xFF>>, 32), 0x1D00FFFF)
  end

  test "meets_target? is false for a malformed compact (never trusts it)" do
    refute Work.meets_target?(:binary.copy(<<0>>, 32), 0x01800000)
  end
end
