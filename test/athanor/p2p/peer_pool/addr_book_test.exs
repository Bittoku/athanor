defmodule Athanor.P2P.PeerPool.AddrBookTest do
  @moduledoc """
  Tests for the pure `Athanor.P2P.PeerPool.AddrBook` reducer (T2.0) — the brain
  of the pool. It decides which addresses to dial next while honoring target
  size, /24 subnet diversity (across both live AND in-flight `dialing` peers),
  and per-address negative cooldown. No process, no IO; time is injected.
  """
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Athanor.P2P.PeerPool.AddrBook

  # Addresses are {ip4_tuple, port}; /24 is the first three octets.
  defp addr(a, b, c, d, port \\ 18_333), do: {{a, b, c, d}, port}

  describe "candidates" do
    test "new/1 carries the target; add_candidates unions and dedups" do
      book =
        AddrBook.new(3)
        |> AddrBook.add_candidates([addr(10, 0, 1, 1), addr(10, 0, 2, 1)])
        |> AddrBook.add_candidates([addr(10, 0, 2, 1), addr(10, 0, 3, 1)])

      # Three distinct candidates, none yet dialing/live.
      assert AddrBook.dial_targets(book, 0) |> length() == 3
    end
  end

  describe "dial_targets" do
    setup do
      book =
        AddrBook.new(2)
        |> AddrBook.add_candidates([
          addr(10, 0, 1, 1),
          addr(10, 0, 1, 2),
          addr(10, 0, 2, 1),
          addr(10, 0, 3, 1)
        ])

      %{book: book}
    end

    test "returns up to (target - used) addresses, all in distinct /24s", %{book: book} do
      targets = AddrBook.dial_targets(book, 0)

      assert length(targets) == 2
      slash24s = Enum.map(targets, fn {{a, b, c, _d}, _p} -> {a, b, c} end)
      assert slash24s == Enum.uniq(slash24s)
    end

    test "excludes an address (and its /24) once it is dialing", %{book: book} do
      book = AddrBook.mark_dialing(book, addr(10, 0, 1, 1))
      targets = AddrBook.dial_targets(book, 0)

      # One slot left; the 10.0.1.x /24 is reserved by the dialing peer, so the
      # only eligible targets are in 10.0.2 / 10.0.3.
      assert length(targets) == 1
      assert [{{10, 0, c, _}, _}] = targets
      assert c in [2, 3]
    end

    test "returns [] when used (live + dialing) is already at target", %{book: book} do
      book =
        book
        |> AddrBook.mark_dialing(addr(10, 0, 1, 1))
        |> AddrBook.mark_dialing(addr(10, 0, 2, 1))

      assert AddrBook.dial_targets(book, 0) == []
    end

    test "excludes a /24 already occupied by a live peer", %{book: book} do
      book =
        book
        |> AddrBook.mark_dialing(addr(10, 0, 1, 1))
        |> AddrBook.promote(addr(10, 0, 1, 1))

      targets = AddrBook.dial_targets(book, 0)
      # 10.0.1.x is live; one slot left → exactly one target from 10.0.2/10.0.3.
      assert length(targets) == 1
      assert [{{10, 0, c, _}, _}] = targets
      assert c in [2, 3]
    end

    test "respects cooldown: an address is excluded until its deadline, then eligible" do
      # Two candidates in distinct /24s; fail the first → cooldown until t=1000.
      book =
        AddrBook.new(4)
        |> AddrBook.add_candidates([addr(10, 0, 1, 1), addr(10, 0, 2, 1)])
        |> AddrBook.mark_dialing(addr(10, 0, 1, 1))
        |> AddrBook.fail_dial(addr(10, 0, 1, 1), 0, 1_000)

      refute addr(10, 0, 1, 1) in AddrBook.dial_targets(book, 500)
      assert addr(10, 0, 1, 1) in AddrBook.dial_targets(book, 1_000)
    end
  end

  describe "dial lifecycle" do
    test "mark_dialing → promote keeps the /24 reserved; release → cooldown frees the address" do
      book =
        AddrBook.new(2)
        |> AddrBook.add_candidates([addr(10, 0, 1, 1), addr(10, 0, 1, 2)])
        |> AddrBook.mark_dialing(addr(10, 0, 1, 1))
        |> AddrBook.promote(addr(10, 0, 1, 1))

      # 10.0.1 is live; the sibling 10.0.1.2 must not be dialable.
      assert AddrBook.dial_targets(book, 0) == []
      assert AddrBook.occupied_slash24s(book) == MapSet.new([{10, 0, 1}])

      # The live peer drops → cooldown; its /24 frees, but the address itself is
      # on cooldown until the deadline.
      book = AddrBook.release(book, addr(10, 0, 1, 1), 0, 1_000)
      assert AddrBook.occupied_slash24s(book) == MapSet.new()
      refute addr(10, 0, 1, 1) in AddrBook.dial_targets(book, 500)
      # The sibling (different address, same freed /24) is dialable now.
      assert addr(10, 0, 1, 2) in AddrBook.dial_targets(book, 500)
    end
  end

  property "no two live-or-dialing addresses ever share a /24" do
    # Generate candidate addresses with deliberately-colliding /24s.
    addr_gen =
      gen all(c <- integer(0..5), d <- integer(1..254)) do
        addr(10, 0, c, d)
      end

    check all(candidates <- uniq_list_of(addr_gen, max_length: 30), target <- integer(1..8)) do
      book = AddrBook.new(target) |> AddrBook.add_candidates(candidates)

      # Fill greedily: dial every selection, then promote half / leave half dialing.
      final =
        Enum.reduce(1..5, book, fn _round, acc ->
          targets = AddrBook.dial_targets(acc, 0)

          targets
          |> Enum.with_index()
          |> Enum.reduce(acc, fn {a, i}, b ->
            b = AddrBook.mark_dialing(b, a)
            if rem(i, 2) == 0, do: AddrBook.promote(b, a), else: b
          end)
        end)

      slash24s = AddrBook.occupied_slash24s(final)
      occupied_count = map_size(occupied(final))

      # Invariant: one /24 per occupied peer, and never more than target.
      assert MapSet.size(slash24s) == occupied_count
      assert occupied_count <= target
    end
  end

  # Test helper mirroring the reducer's notion of "occupied" (live ∪ dialing)
  # for the property's count assertion.
  defp occupied(book) do
    Map.merge(
      Map.new(MapSet.to_list(book.dialing), &{&1, :dialing}),
      Map.new(Map.keys(book.live), &{&1, :live})
    )
  end
end
