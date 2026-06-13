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

  test "a transient nil boundary hash leaves no unanchored singleton; a later tick captures + catches up" do
    # note-1049 B1: capture must be atomic with anchor recording. If `rpc_hash_at`
    # for the boundary height is transiently unavailable at startup, the singleton
    # bootstrap row must NOT be persisted (it is capture-once/idempotent) — otherwise
    # a fresh index is left anchorless forever and fails closed. The capture must
    # retry on a later tick once the hash resolves.
    assert Bootstrap.fetch() == nil

    node = for h <- 103..105, into: %{}, do: {h, hex(h)}
    {:ok, gate} = Agent.start_link(fn -> false end)

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

    proc = start_supervised!({BlockProcessor, []})

    pid =
      start_supervised!({
        TipController,
        # No configured hash → the boundary hash must come from `rpc_hash_at`, which
        # is gated nil until we open it below.
        name: TipController,
        bootstrap_height: 103,
        bootstrap_hash: nil,
        rpc_height: fn -> {:ok, 105} end,
        rpc_hash_at: fn h ->
          if h == 103 and not Agent.get(gate, & &1), do: nil, else: node[h]
        end,
        apply_fun: apply_fun,
        processor: proc,
        tick_interval_ms: 60_000
      })

    _ = drain(pid)

    # First startup: boundary hash unavailable → nothing persisted, nothing anchored,
    # and the reconcile cycle is gated (no spurious catch-up from height 1).
    assert Bootstrap.fetch() == nil
    assert Repo.aggregate(BlockProcessContext, :count) == 0

    # The hash resolves; a tick retries capture and then catches up contiguously.
    Agent.update(gate, fn _ -> true end)
    send(pid, :tick)
    _ = drain(pid)

    assert Bootstrap.fetch() == %{height: 103, hash: hex(103)}
    heights = Repo.all(BlockProcessContext) |> Enum.map(& &1.height) |> Enum.sort()
    assert heights == [103, 104, 105]
  end

  # ── note-1053: capture + anchor must be atomic and self-healing ──

  test "capture_bootstrap rolls back the singleton when the anchor insert fails (atomic)" do
    proc = start_supervised!({BlockProcessor, []})
    assert Bootstrap.fetch() == nil

    # A blank id makes the in-transaction anchor changeset fail (validate_required),
    # simulating an invalid/failed anchor insert. The boundary `Bootstrap.ensure` must
    # roll back with it — no persisted-but-unanchored singleton can survive.
    assert {:error, {:anchor_insert_failed, _}} = BlockProcessor.capture_bootstrap(proc, 103, "")

    assert Bootstrap.fetch() == nil
    assert Repo.aggregate(BlockProcessContext, :count) == 0
  end

  test "capture_bootstrap repairs a stranded singleton (captured, never anchored) and is idempotent" do
    proc = start_supervised!({BlockProcessor, []})

    # Simulate a prior crash: the singleton was persisted but the anchor never was.
    Bootstrap.ensure(103, hex(103))
    assert Repo.aggregate(BlockProcessContext, :count) == 0

    # Capture detects the empty index and inserts the missing anchor (self-healing).
    assert {:ok, 103} = BlockProcessor.capture_bootstrap(proc, 103, hex(103))
    assert Repo.get(BlockProcessContext, hex(103)).height == 103

    # Idempotent: a second call on the now-anchored index is a no-op.
    assert {:ok, 103} = BlockProcessor.capture_bootstrap(proc, 103, hex(103))
    assert Repo.aggregate(BlockProcessContext, :count) == 1
  end

  test "a controller restart over a stranded singleton repairs the anchor before reconciling" do
    # The exact restart risk from note-1053: `Bootstrap.fetch/0` is non-nil (singleton
    # persisted) but the index is empty (anchor lost to a crash). The controller must
    # repair the anchor *before* any reconcile runs, not reconcile from height 0.
    node = for h <- 103..105, into: %{}, do: {h, hex(h)}

    proc = start_supervised!({BlockProcessor, []})
    Bootstrap.ensure(103, hex(103))
    assert Repo.aggregate(BlockProcessContext, :count) == 0

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
        name: TipController,
        rpc_height: fn -> {:ok, 105} end,
        rpc_hash_at: fn h -> node[h] end,
        apply_fun: apply_fun,
        processor: proc,
        tick_interval_ms: 60_000
      })

    _ = drain(pid)

    # Anchor repaired (103 recorded from the stranded boundary), then 104 + 105 caught
    # up contiguously — no reconcile ran from an empty/unanchored index.
    heights = Repo.all(BlockProcessContext) |> Enum.map(& &1.height) |> Enum.sort()
    assert heights == [103, 104, 105]
  end
end
