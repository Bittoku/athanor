defmodule Athanor.Indexer.TipControllerBootstrapTest do
  @moduledoc """
  Phase 7 F7.2 (T7.S / Hermes !20 note 1045 B1) — a fresh install must capture the
  bootstrap boundary and anchor the index **at startup**, with no manual
  `Bootstrap.ensure/2`. Otherwise the no-gap predecessor guard refuses every block
  forever and the node can't index.

  Starts with an empty `indexer_bootstrap` table **and** empty
  `block_process_contexts`, starts the controller, and proves the boundary row is
  captured, the anchor block is recorded, and the first cycle catches up
  contiguously from the boundary (not from height 1).
  """
  use Athanor.DataCase, async: false

  alias Athanor.Indexer.{BlockProcessor, Bootstrap, TipController}
  alias Athanor.Schema.BlockProcessContext

  defp hex(h), do: Base.encode16(<<h::16>>, case: :lower)

  defp drain(pid, n \\ 30)
  defp drain(pid, 0), do: :sys.get_state(pid)

  defp drain(pid, n) do
    s = :sys.get_state(pid)
    if s.machine.in_flight or s.machine.pending, do: drain(pid, n - 1), else: s
  end

  test "captures the bootstrap boundary + anchor at startup and catches up contiguously" do
    assert Bootstrap.fetch() == nil
    assert Repo.aggregate(BlockProcessContext, :count) == 0

    node = for h <- 103..105, into: %{}, do: {h, hex(h)}

    proc = start_supervised!({BlockProcessor, []})

    # A test apply that records the catch-up blocks as contexts (no RPC), so the
    # default DB-backed local seams advance. Heights are contiguous from local+1.
    apply_fun = fn _p, %{rollback_to: rb, connect: connect} ->
      start = (rb || BlockProcessor.last_processed_height()) + 1

      heights =
        if connect == [], do: [], else: Enum.to_list(start..(start + length(connect) - 1))

      for h <- heights do
        Repo.insert!(%BlockProcessContext{
          id: node[h],
          height: h,
          processed_at: DateTime.utc_now() |> DateTime.truncate(:second)
        })
      end

      {:ok, List.last(heights) || start - 1}
    end

    pid =
      start_supervised!({
        TipController,
        # Configured boundary below the node tip → must record the anchor (103)
        # then catch up to 105 — contiguously, automatically.
        name: TipController,
        bootstrap_height: 103,
        bootstrap_hash: hex(103),
        rpc_height: fn -> {:ok, 105} end,
        rpc_hash_at: fn h -> node[h] end,
        apply_fun: apply_fun,
        processor: proc,
        tick_interval_ms: 60_000
      })

    _ = drain(pid)

    # Boundary captured once, with no manual ensure.
    assert Bootstrap.fetch() == %{height: 103, hash: hex(103)}
    # The anchor block (103) was recorded, then 104 + 105 caught up contiguously
    # (the catch-up here uses the injected apply, which records contexts directly).
    heights = Repo.all(BlockProcessContext) |> Enum.map(& &1.height) |> Enum.sort()
    assert heights == [103, 104, 105]
    assert Repo.aggregate(BlockProcessContext, :max, :height) == 105
  end

  test "with no configured boundary, anchors at the current RPC node tip" do
    proc = start_supervised!({BlockProcessor, []})

    pid =
      start_supervised!(
        {TipController,
         name: TipController,
         rpc_height: fn -> {:ok, 200} end,
         rpc_hash_at: fn h -> hex(h) end,
         apply_fun: fn _p, _a -> {:ok, 200} end,
         processor: proc,
         tick_interval_ms: 60_000}
      )

    _ = drain(pid)

    assert Bootstrap.fetch() == %{height: 200, hash: hex(200)}
    assert Repo.get(BlockProcessContext, hex(200)).height == 200
  end
end
