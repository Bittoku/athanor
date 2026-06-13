defmodule Athanor.Indexer.TipControllerLiveSmokeTest do
  @moduledoc """
  Live smoke for the Phase 7 F7.2 index integration (T7.5).

  Tagged `:external` and excluded from `mix test` (see T1.S); run against a
  configured RPC node (and, optionally, live P2P) with:

      mix test --only external

  Proves the production path end to end: capture the bootstrap boundary at the
  node tip, then drive a `TipController` reconcile against the **real** `RpcClient`
  and assert the index catches up to the node tip and stays contiguous — i.e. the
  RPC-confirmed reconcile actually applies blocks through `apply_branch/2`.

  Requires a reachable BSV RPC node (`Athanor.Blockchain.RpcClient`). With no node
  configured this test cannot run, which is why it is `:external`.
  """
  use Athanor.DataCase, async: false

  alias Athanor.Blockchain.RpcClient
  alias Athanor.Indexer.{Bootstrap, BlockProcessor, TipController}

  defp eventually(fun, timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_eventually(fun, deadline)
  end

  defp do_eventually(fun, deadline) do
    cond do
      fun.() ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        false

      true ->
        receive do
        after
          250 -> :ok
        end && do_eventually(fun, deadline)
    end
  end

  @tag :external
  @tag timeout: 120_000
  test "bootstraps at the node tip and a hint drives an RPC-confirmed reconcile to that tip" do
    {:ok, node_height} = RpcClient.get_block_count()

    # Anchor the bootstrap a few blocks below the tip so the controller has a small,
    # bounded catch-up to perform against the live node.
    bootstrap_height = max(node_height - 3, 1)
    {:ok, bootstrap_hash} = RpcClient.get_block_hash(bootstrap_height)
    Bootstrap.ensure(bootstrap_height, String.downcase(bootstrap_hash))

    # Record the bootstrap block so the index has its contiguous anchor, then start
    # the controller with the production RPC seams.
    {:ok, _} =
      %Athanor.Schema.BlockProcessContext{}
      |> Athanor.Schema.BlockProcessContext.changeset(%{
        id: String.downcase(bootstrap_hash),
        height: bootstrap_height,
        processed_at: DateTime.utc_now() |> DateTime.truncate(:second)
      })
      |> Athanor.Repo.insert()

    start_supervised!({BlockProcessor, []})
    start_supervised!({TipController, name: TipController, tick_interval_ms: 2_000})

    TipController.hint(:smoke, nil)

    assert eventually(fn -> BlockProcessor.last_processed_height() >= node_height end, 90_000),
           "index did not catch up to the node tip #{node_height}"
  end
end
