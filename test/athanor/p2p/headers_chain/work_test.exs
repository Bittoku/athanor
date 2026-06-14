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
  import Bitwise

  alias Athanor.P2P.Codec.Hash
  alias Athanor.P2P.HeadersChain.Work

  # mainnet/testnet consensus pow-limit (max target) compact.
  @pow_limit 0x1D00FFFF

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

  describe "valid_pow?/3 (consensus pow-limit + target gate)" do
    test "rejects an over-limit (easier-than-consensus) target even when the hash meets it" do
      # 0x207fffff (regtest-easy) decodes to a target far above the network
      # pow-limit; a peer must not credit work for it on mainnet/testnet.
      assert Work.meets_target?(:binary.copy(<<0>>, 32), 0x207FFFFF)
      refute Work.valid_pow?(:binary.copy(<<0>>, 32), 0x207FFFFF, @pow_limit)
    end

    test "accepts the testnet genesis header (within limit, hash meets target)" do
      {:ok, display} =
        Base.decode16("000000000933EA01AD0EE984209779BAAEC3CED90FA3F408719526F8D77F4943",
          case: :mixed
        )

      wire = Hash.display_to_wire(display)
      assert Work.valid_pow?(wire, @pow_limit, @pow_limit)
    end

    test "rejects a within-limit target the hash does NOT meet" do
      refute Work.valid_pow?(:binary.copy(<<0xFF>>, 32), @pow_limit, @pow_limit)
    end

    test "rejects a malformed compact regardless of the limit" do
      refute Work.valid_pow?(:binary.copy(<<0>>, 32), 0x00000000, @pow_limit)
      refute Work.valid_pow?(:binary.copy(<<0>>, 32), 0x01800000, @pow_limit)
    end
  end

  describe "target_to_compact/1 (F7.1 T7.1.0, canonical nBits encoder)" do
    test "encodes the known pow-limit and a harder target to their compacts" do
      {:ok, pow_limit_target} = Work.compact_to_target(0x1D00FFFF)
      assert Work.target_to_compact(pow_limit_target) == 0x1D00FFFF

      {:ok, harder} = Work.compact_to_target(0x1C00FFFF)
      assert Work.target_to_compact(harder) == 0x1C00FFFF
    end

    test "is the left inverse of compact_to_target for canonical compacts" do
      for compact <- [0x1D00FFFF, 0x1C00FFFF, 0x1B0404CB, 0x1903A30C, 0x180696F2, 0x05009234] do
        {:ok, target} = Work.compact_to_target(compact)

        assert Work.target_to_compact(target) == compact,
               "round-trip failed for 0x#{Integer.to_string(compact, 16)}"
      end
    end

    test "emits only the canonical form — a non-canonical compact for the same target is not produced" do
      # 0x02007f00 and 0x017f0000 both decode to target 127, but only 0x017f0000
      # is canonical (minimal byte length). The encoder must yield 0x017f0000, so
      # a candidate header carrying the raw 0x02007f00 fails exact-bits equality (I1).
      assert {:ok, 127} = Work.compact_to_target(0x02007F00)
      assert {:ok, 127} = Work.compact_to_target(0x017F0000)
      assert Work.target_to_compact(127) == 0x017F0000
      refute Work.target_to_compact(127) == 0x02007F00
    end

    test "applies the sign-bit-avoidance shift (high mantissa byte never 0x80+)" do
      # A target whose top byte is 0x80 must be encoded with a leading zero byte and
      # a bumped exponent, never a mantissa with 0x800000 set (which decodes to :error).
      target = 0x80 * Integer.pow(2, 8 * 30)
      compact = Work.target_to_compact(target)
      assert (compact &&& 0x00800000) == 0
      assert {:ok, ^target} = Work.compact_to_target(compact)
    end

    test "rejects a non-positive or out-of-range target" do
      assert Work.target_to_compact(0) == :error
      assert Work.target_to_compact(-1) == :error
      assert Work.target_to_compact(Integer.pow(2, 256)) == :error
    end
  end
end
