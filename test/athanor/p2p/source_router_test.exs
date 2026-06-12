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

  describe "route/3 runner" do
    # raw_tx_fetch default = {:p2p, [:rpc, :junglebus, :whatsonchain]}.

    test "a primary :ok short-circuits — fallbacks are never attempted" do
      {:ok, _} = Agent.start_link(fn -> [] end, name: :route_log)
      record = fn p -> Agent.update(:route_log, &[p | &1]) end

      result =
        SourceRouter.route(
          :raw_tx_fetch,
          fn p ->
            record.(p)
            if p == :p2p, do: {:ok, "hit"}, else: :miss
          end,
          p2p_available?: true
        )

      assert result == {:ok, "hit"}
      assert Agent.get(:route_log, & &1) == [:p2p]
    end

    test "a :p2p primary is skipped (instant miss) when p2p_available? is false" do
      attempted =
        SourceRouter.route(
          :raw_tx_fetch,
          fn
            :p2p -> flunk(":p2p must not be attempted when unavailable")
            :rpc -> {:ok, "rpc"}
            _ -> :miss
          end,
          p2p_available?: false
        )

      assert attempted == {:ok, "rpc"}
    end

    test "advances through fallbacks in order on :miss, returning the first :ok" do
      {:ok, _} = Agent.start_link(fn -> [] end, name: :route_order)

      result =
        SourceRouter.route(
          :raw_tx_fetch,
          fn p ->
            Agent.update(:route_order, &(&1 ++ [p]))
            if p == :junglebus, do: {:ok, "jb"}, else: :miss
          end,
          p2p_available?: true
        )

      assert result == {:ok, "jb"}
      assert Agent.get(:route_order, & &1) == [:p2p, :rpc, :junglebus]
    end

    test "all providers miss → :miss" do
      assert SourceRouter.route(:raw_tx_fetch, fn _ -> :miss end, p2p_available?: true) == :miss
    end

    test "all providers error → the last error is returned" do
      assert SourceRouter.route(:raw_tx_fetch, fn p -> {:error, p} end, p2p_available?: true) ==
               {:error, :whatsonchain}
    end

    test "a provider that RAISES is normalized to an error and routing continues" do
      result =
        SourceRouter.route(
          :raw_tx_fetch,
          fn
            :p2p -> raise "boom"
            :rpc -> {:ok, "rpc"}
            _ -> :miss
          end,
          p2p_available?: true
        )

      assert result == {:ok, "rpc"}
    end

    test "a provider that EXITS (e.g. a GenServer.call to a down process) does not crash routing" do
      result =
        SourceRouter.route(
          :raw_tx_fetch,
          fn
            :p2p -> exit(:noproc)
            :rpc -> {:ok, "rpc"}
            _ -> :miss
          end,
          p2p_available?: true
        )

      assert result == {:ok, "rpc"}
    end

    test "if every provider crashes, the last normalized error is returned (no exit escapes)" do
      assert {:error, {:provider_exited, :whatsonchain, _}} =
               SourceRouter.route(:raw_tx_fetch, fn _ -> exit(:down) end, p2p_available?: true)
    end

    test "default gate fails closed when P2P is enabled but PeerRegistry is unavailable" do
      # P2P enabled, but no PeerRegistry process → PeerRegistry.pids/0 exits. The
      # availability gate must treat that as 'no peers' and route to RPC, not crash.
      Application.put_env(:athanor, Athanor.P2P, enabled: true)
      on_exit(fn -> Application.delete_env(:athanor, Athanor.P2P) end)

      result =
        SourceRouter.route(:raw_tx_fetch, fn
          :p2p -> flunk(":p2p must be skipped when the registry is unavailable")
          :rpc -> {:ok, "rpc"}
          _ -> :miss
        end)

      assert result == {:ok, "rpc"}
    end
  end
end
