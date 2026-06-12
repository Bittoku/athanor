defmodule Athanor.P2P.WatchlistTest do
  @moduledoc """
  Tests for `Athanor.P2P.Watchlist` (T3.0) — the cheap `:ets` prefilter that
  rejects obviously-irrelevant txs at P2P ingest rate. It must be a strict
  **superset** of `TransactionFilter.matches?/1`, which matches on watched
  P2PKH/P2MPKH addresses **and** watched STAS/STAS3 tokens. So the prefilter
  accepts a tx if any output's hash160 8-byte prefix is watched **or** any
  output is STAS-template-shaped (the token superset, !10 review blocker 1).
  """
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Athanor.P2P.Watchlist
  alias BSV.Tokens.Script.{Stas3Builder, Templates}

  defp p2pkh_script(<<h160::binary-20>>),
    do: elem(BSV.Script.from_binary(<<0x76, 0xA9, 0x14, h160::binary-20, 0x88, 0xAC>>), 1)

  defp tx_with(script),
    do: %BSV.Transaction{
      outputs: [%BSV.Transaction.Output{satoshis: 1000, locking_script: script}]
    }

  defp p2pkh_tx(h160), do: tx_with(p2pkh_script(h160))

  defp p2mpkh_tx(mpkh) do
    {:ok, script} = BSV.Script.from_binary(Templates.p2mpkh_locking_script(mpkh))
    tx_with(script)
  end

  defp stas3_tx(proto) do
    {:ok, script} =
      Stas3Builder.build_stas3_locking_script(
        :binary.copy(<<0xAA>>, 20),
        proto,
        nil,
        false,
        %BSV.Tokens.ScriptFlags{},
        [],
        []
      )

    tx_with(script)
  end

  defp watch_address(wl, <<h160::binary-20>>),
    do: Watchlist.put_address(wl, BSV.Base58.check_encode(h160, 0x00))

  test "accepts a watched-address P2PKH output, rejects an unwatched one" do
    wl = Watchlist.new()
    watched = :binary.copy(<<0x11>>, 20)
    watch_address(wl, watched)

    assert Watchlist.maybe_relevant?(wl, p2pkh_tx(watched))
    refute Watchlist.maybe_relevant?(wl, p2pkh_tx(:binary.copy(<<0x22>>, 20)))
  end

  test "accepts a watched-address paid via P2MPKH" do
    wl = Watchlist.new()
    mpkh = :binary.copy(<<0x33>>, 20)
    watch_address(wl, mpkh)

    assert Watchlist.maybe_relevant?(wl, p2mpkh_tx(mpkh))
  end

  test "accepts a STAS token output even with no watched address (token superset)" do
    wl = Watchlist.new()
    # No watched addresses at all — a token tx must still pass to matches?/1.
    proto = :binary.copy(<<0x55>>, 20)

    assert Watchlist.maybe_relevant?(wl, stas3_tx(proto))
  end

  test "rejects an unrelated, non-STAS tx" do
    wl = Watchlist.new()
    watch_address(wl, :binary.copy(<<0x11>>, 20))

    refute Watchlist.maybe_relevant?(wl, p2pkh_tx(:binary.copy(<<0x99>>, 20)))
  end

  property "never drops a watched-address output (address-path superset)" do
    check all(
            others <- list_of(binary(length: 20), max_length: 4),
            target <- binary(length: 20)
          ) do
      wl = Watchlist.new()
      watch_address(wl, target)
      for o <- others, o != target, do: :ok

      # A tx paying the watched hash160 is never filtered out.
      assert Watchlist.maybe_relevant?(wl, p2pkh_tx(target))
    end
  end
end
