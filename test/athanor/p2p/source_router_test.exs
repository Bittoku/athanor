defmodule Athanor.P2P.SourceRouterTest do
  @moduledoc """
  Tests for `Athanor.P2P.SourceRouter` (Phase 5 T5.0, §A) — the pure, config-driven
  capability→provider resolver. `resolve/1` returns `{primary, fallbacks}` from the
  honest default table, deep-merged with any `config :athanor, Athanor.P2P.SourceRouter`
  override. No process, no IO, no peer awareness (the caller gates `:p2p` on
  `p2p_available?` — that is the route runner's job, T5.4).

  `async: false` — the override case mutates global application env.
  """
  use ExUnit.Case, async: false

  alias Athanor.P2P.SourceRouter

  # The §A honesty contract — the single source of truth for default routing.
  @defaults %{
    raw_tx_fetch: {:p2p, [:rpc, :junglebus, :whatsonchain]},
    broadcast: {:p2p, [:rpc]},
    realtime_ingest: {:p2p, [:zmq, :junglebus]},
    validation_fetch: {:rpc, []},
    block_backfill: {:rpc, [:junglebus]},
    historical_scan: {:whatsonchain, []},
    balance_utxo_fetch: {:whatsonchain, [:bitails]}
  }

  test "resolve/1 returns the documented default {primary, fallbacks} for every capability" do
    for {capability, expected} <- @defaults do
      assert SourceRouter.resolve(capability) == expected,
             "capability #{inspect(capability)} should resolve to #{inspect(expected)}"
    end
  end

  test "only raw_tx_fetch, broadcast, realtime_ingest are P2P-primary (honesty guard)" do
    p2p_primary =
      for {cap, _} <- @defaults,
          elem(SourceRouter.resolve(cap), 0) == :p2p,
          into: MapSet.new(),
          do: cap

    assert p2p_primary == MapSet.new([:raw_tx_fetch, :broadcast, :realtime_ingest])
  end

  test "validation/block_backfill/historical/balance stay REST/RPC (no P2P primary)" do
    assert {:rpc, _} = SourceRouter.resolve(:validation_fetch)
    assert {:rpc, _} = SourceRouter.resolve(:block_backfill)
    assert {:whatsonchain, _} = SourceRouter.resolve(:historical_scan)
    assert {:whatsonchain, _} = SourceRouter.resolve(:balance_utxo_fetch)
  end

  test "an unknown capability raises (programmer error, not a runtime fallthrough)" do
    assert_raise ArgumentError, fn -> SourceRouter.resolve(:nonsense) end
  end

  test "config deep-merges per capability: an override changes only that capability" do
    Application.put_env(:athanor, SourceRouter, routes: %{broadcast: {:rpc, []}})
    on_exit(fn -> Application.delete_env(:athanor, SourceRouter) end)

    # Overridden capability reflects config...
    assert SourceRouter.resolve(:broadcast) == {:rpc, []}
    # ...every other capability keeps its default.
    assert SourceRouter.resolve(:raw_tx_fetch) == {:p2p, [:rpc, :junglebus, :whatsonchain]}
    assert SourceRouter.resolve(:validation_fetch) == {:rpc, []}
  end
end
