defmodule Athanor.P2P.HeadersChain.DaaTest do
  @moduledoc """
  Tests for `Athanor.P2P.HeadersChain.Daa` (Phase 7 F7.1, §3) — the pure BSV
  cw-144 difficulty core.

  The decisive test (T7.1.4) is the **mainnet golden vectors**: a contiguous run
  of real BSV mainnet headers (heights 699840..700000, captured from WhatsOnChain
  into `test/support/fixtures/daa_mainnet_headers.json`). For every candidate with
  a full 147-ancestor window, `expected_bits/3` must reproduce the block's actual
  on-chain `nBits`. This pins the consensus details — in particular the
  `first = suitable(P-144)` anchor (deepest ancestor `P-146`) — against the real
  chain rather than a re-derivation.
  """
  use ExUnit.Case, async: true

  alias Athanor.P2P.HeadersChain.{Daa, Work}

  @pow_limit 0x1D00FFFF

  describe "suitable/1 (median-of-three by time)" do
    test "returns the median-time node regardless of input order" do
      a = %{time: 100, cum_work: 1}
      b = %{time: 200, cum_work: 2}
      c = %{time: 300, cum_work: 3}

      for triple <- permutations([a, b, c]) do
        assert Daa.suitable(triple) == b,
               "median wrong for #{inspect(Enum.map(triple, & &1.time))}"
      end
    end

    test "is robust to a single skewed (out-of-order) timestamp" do
      # b1 has a wildly early time; the median is still the true middle by time.
      assert Daa.suitable([
               %{time: 500, cum_work: 1},
               %{time: 1, cum_work: 2},
               %{time: 600, cum_work: 3}
             ]).time ==
               500
    end

    test "an equal-timestamp tie selects the consensus {B,B-1,B-2} element (blocker 1)" do
      # B and B-2 share a timestamp; B-1 is earlier; B vs B-2 have distinct cum_work.
      # bitcoin-abc feeds [B, B-1, B-2] and its swap-on-`>`-only network returns B.
      b = %{time: 2000, cum_work: 9_000}
      b1 = %{time: 1000, cum_work: 8_500}
      b2 = %{time: 2000, cum_work: 8_000}

      assert Daa.suitable([b, b1, b2]) == b
      # The reversed input order (the pre-fix bug) would have returned B-2 instead.
      assert Daa.suitable([b2, b1, b]) == b2
    end
  end

  describe "expected_bits/3 window handling" do
    test "returns :insufficient_window when an ancestor is missing" do
      parent = %{time: 10, cum_work: 100, height: 5}
      # ancestor_fun that only knows the parent itself.
      anc = fn _node, n -> if n == 0, do: parent, else: nil end
      assert Daa.expected_bits(parent, anc, @pow_limit) == {:error, :insufficient_window}
    end

    test "the `last` endpoint window is read in consensus {P,P-1,P-2} order (blocker 1)" do
      # The `last` triple {P, P-1, P-2} ties P and P-2 on time (P-1 earlier), with
      # cum_work large and DISTINCT so the retarget is below the pow-limit (unclamped)
      # and the chosen endpoint is observable in `expected_bits`. Consensus selects P;
      # the pre-fix reversed window order would have selected P-2.
      p = %{time: 1_000_000, cum_work: 4_000_000_000_000}
      p1 = %{time: 900_000, cum_work: 3_500_000_000_000}
      p2 = %{time: 1_000_000, cum_work: 3_000_000_000_000}

      # The `first` triple {P-144, P-145, P-146} is strictly ordered (unambiguous);
      # its median (P-145) anchors the retarget.
      f0 = %{time: 899_000, cum_work: 0}
      f1 = %{time: 898_000, cum_work: 0}
      f2 = %{time: 897_000, cum_work: 0}

      nodes = %{0 => p, 1 => p1, 2 => p2, 144 => f0, 145 => f1, 146 => f2}
      anc = fn _node, n -> Map.get(nodes, n) end

      {:ok, pow_target} = Work.compact_to_target(@pow_limit)
      first = Daa.suitable([f0, f1, f2])
      consensus = Daa.compute_target(first, p, pow_target)
      reversed = Daa.compute_target(first, p2, pow_target)

      # The bug is only observable if the two endpoints give different bits.
      refute consensus == reversed
      assert Daa.expected_bits(p, anc, @pow_limit) == {:ok, consensus}
    end
  end

  describe "mainnet cw-144 golden vectors (T7.1.4)" do
    setup do
      fixture =
        "test/support/fixtures/daa_mainnet_headers.json"
        |> File.read!()
        |> Jason.decode!()

      headers = fixture["headers"]
      low = fixture["low_height"]
      high = fixture["high_height"]

      # Cumulative work from the fixture's low edge (only deltas matter to cw-144).
      {by_height, _} =
        Enum.reduce(headers, {%{}, 0}, fn h, {acc, cum} ->
          bits = String.to_integer(h["bits"], 16)
          {:ok, w} = Work.work(bits)
          cum = cum + w
          node = %{time: h["time"], cum_work: cum, height: h["height"], bits: bits}
          {Map.put(acc, h["height"], node), cum}
        end)

      ancestor_fun = fn node, n -> Map.get(by_height, node.height - n) end
      %{by_height: by_height, ancestor_fun: ancestor_fun, low: low, high: high}
    end

    test "expected_bits reproduces each fully-windowed block's actual nBits", ctx do
      # A candidate at height H needs ancestors down to P-146 = H-147.
      candidates = (ctx.low + 147)..ctx.high

      results =
        for h <- candidates do
          parent = ctx.by_height[h - 1]
          {:ok, got} = Daa.expected_bits(parent, ctx.ancestor_fun, @pow_limit)
          {h, got, ctx.by_height[h].bits}
        end

      assert results != []

      for {h, got, actual} <- results do
        assert got == actual,
               "height #{h}: expected_bits 0x#{Integer.to_string(got, 16)} != actual 0x#{Integer.to_string(actual, 16)}"
      end
    end
  end

  defp permutations([]), do: [[]]
  defp permutations(list), do: for(e <- list, p <- permutations(list -- [e]), do: [e | p])
end
