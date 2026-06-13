defmodule Athanor.Indexer.TipController do
  @moduledoc """
  Phase 7 F7.2 (T7.3) — the single index-tip mutation owner.

  The thin node tracks the chain tip in near-real-time **without ever trusting an
  unconfirmed peer**: the RPC node is the sole authority that mutates the index, and
  the P2P headers chain plus the ZMQ / JungleBus listeners are **advisory hints**
  that make the RPC reconcile happen sooner than the periodic poll. This GenServer
  is the only thing that calls `BlockProcessor.apply_branch/2`.

  It owns the pure `Machine` (when to run a cycle; authority phase) and runs RPC
  **reconcile cycles** via the pure `Reconcile` core. A cycle:

    1. reads the node height (`:rpc_height`, fail-closed) and reconciles **by hash**
       against the node (`Reconcile.reconcile_plan/4`) using the local
       (`block_process_contexts`) and node (`:rpc_hash_at`) seams;
    2. turns the plan into a contiguous `apply_branch/2` argument
       (`Reconcile.branch_for/3`) and dispatches it to `BlockProcessor`;
    3. folds the outcome into the `Machine` as a `{:cycle_result, …}`:
       `:synced` (at the node tip), `:progressed` (applied a branch — schedule a
       follow-up), or `:deferred` (RPC error / unproven ancestor / failed apply —
       wait for the next trigger). The follow-up is a self-scheduled `:run_cycle`,
       so each message does one bounded cycle and the GenServer stays responsive.

  Hints (`hint/2`, `notify_tip/2`) and the periodic `:tick` are coalesced by the
  `Machine`: at most one cycle is in flight; a trigger during a cycle schedules one
  follow-up.

  Injected collaborators (defaults are the production seams): `:rpc_height`
  (`-> {:ok, h} | {:error, _}`), `:rpc_hash_at` (`height -> hex | nil`),
  `:local_height` (`-> non_neg_integer`), `:local_hash_at` (`height -> hex | nil`),
  `:apply_fun` (`(processor, arg) -> apply_branch result`), `:processor`, `:batch`,
  `:tick_interval_ms`.
  """

  use GenServer
  require Logger

  alias Athanor.Blockchain.RpcClient
  alias Athanor.Indexer.{BlockProcessor, Bootstrap, Reconcile}
  alias Athanor.Indexer.TipController.Machine
  alias Athanor.Repo
  alias Athanor.Schema.BlockProcessContext
  import Ecto.Query

  ## ── Client API ──

  @doc "Starts the controller. See the module doc for options."
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    {name, opts} = Keyword.pop(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  An advisory hint from a realtime producer (`:p2p | :zmq | :junglebus`) that the
  tip may have moved. Coalesced into one RPC-confirmed reconcile cycle; never
  mutates the index directly. `candidate_tip_hash` is advisory only. Always targets
  the registered controller (`__MODULE__`).
  """
  @spec hint(atom(), binary() | nil) :: :ok
  def hint(source, _candidate_tip_hash \\ nil) when is_atom(source) do
    GenServer.cast(__MODULE__, {:hint, source})
  end

  @doc """
  The `HeadersChain` `:on_tip` sink: a P2P tip event is treated as a `:p2p` hint
  (the candidate tip is advisory). The chain's reorg detection thus *accelerates*
  the RPC reconcile without granting P2P any index-mutation authority.
  """
  @spec notify_tip(GenServer.server(), tuple()) :: :ok
  def notify_tip(server \\ __MODULE__, _event), do: GenServer.cast(server, {:hint, :p2p})

  ## ── Server callbacks ──

  @impl true
  def init(opts) do
    state = %{
      machine: Machine.new(),
      rpc_height: Keyword.get(opts, :rpc_height, &default_rpc_height/0),
      rpc_hash_at: Keyword.get(opts, :rpc_hash_at, &default_rpc_hash_at/1),
      local_height: Keyword.get(opts, :local_height, &default_local_height/0),
      local_hash_at: Keyword.get(opts, :local_hash_at, &default_local_hash_at/1),
      apply_fun: Keyword.get(opts, :apply_fun, &BlockProcessor.apply_branch/2),
      anchor_fun: Keyword.get(opts, :anchor_fun, &BlockProcessor.record_bootstrap_anchor/3),
      bootstrap_fetch: Keyword.get(opts, :bootstrap_fetch, &Bootstrap.fetch/0),
      bootstrap_ensure: Keyword.get(opts, :bootstrap_ensure, &Bootstrap.ensure/2),
      processor: Keyword.get(opts, :processor, BlockProcessor),
      batch: Keyword.get(opts, :batch, 10),
      tick_interval_ms: Keyword.get(opts, :tick_interval_ms, :timer.minutes(2)),
      # The bootstrap boundary: an explicit configured height/hash, else the current
      # RPC node tip (resolved + persisted once at startup, see `ensure_bootstrap/1`).
      bootstrap_height:
        Keyword.get(
          opts,
          :bootstrap_height,
          Application.get_env(:athanor, Athanor.Indexer, [])[:bootstrap_height]
        ),
      bootstrap_hash:
        Keyword.get(
          opts,
          :bootstrap_hash,
          Application.get_env(:athanor, Athanor.Indexer, [])[:bootstrap_hash]
        )
    }

    schedule_tick(state.tick_interval_ms)
    # Capture the bootstrap boundary before the first cycle (note-1045 B1).
    {:ok, state, {:continue, :ensure_bootstrap}}
  end

  @impl true
  def handle_continue(:ensure_bootstrap, state) do
    state = ensure_bootstrap(state)
    # Kick the first reconcile now that the index is anchored.
    {:noreply, advance(state, :tick)}
  end

  @impl true
  def handle_cast({:hint, source}, state) do
    {:noreply, advance(state, {:hint, source})}
  end

  def handle_cast(_msg, state), do: {:noreply, state}

  @impl true
  def handle_info(:tick, state) do
    # Retry the bootstrap capture if it deferred at startup (e.g. RPC was down).
    state = ensure_bootstrap(state)
    state = advance(state, :tick)
    schedule_tick(state.tick_interval_ms)
    {:noreply, state}
  end

  def handle_info(:run_cycle, state) do
    result = run_cycle(state)
    {:noreply, advance(state, {:cycle_result, result})}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  ## ── Private ──

  # Capture the bootstrap boundary once (note-1045 B1): resolve the configured
  # height/hash (or the current RPC node tip), persist it, and — on a fresh index —
  # record the anchor block so the predecessor guard has its contiguous start. A
  # no-op if already captured; defers (retried on tick) if RPC is unavailable.
  defp ensure_bootstrap(state) do
    case state.bootstrap_fetch.() do
      nil ->
        case resolve_bootstrap(state) do
          {:ok, height, hash} ->
            state.bootstrap_ensure.(height, hash)

            if is_binary(hash) and state.local_height.() == 0,
              do: state.anchor_fun.(state.processor, height, hash)

            state

          :defer ->
            state
        end

      _existing ->
        state
    end
  end

  # The configured boundary wins; otherwise anchor at the current RPC node tip.
  defp resolve_bootstrap(%{bootstrap_height: h} = state) when is_integer(h) do
    {:ok, h, state.bootstrap_hash || state.rpc_hash_at.(h)}
  end

  defp resolve_bootstrap(state) do
    case safe_rpc_height(state) do
      {:ok, tip} -> {:ok, tip, state.rpc_hash_at.(tip)}
      :error -> :defer
    end
  end

  # Step the Machine by an event and execute the actions it returns.
  defp advance(state, event) do
    {machine, actions} = Machine.step(state.machine, event)
    run_actions(actions, %{state | machine: machine})
  end

  defp run_actions(actions, state) do
    Enum.reduce(actions, state, fn
      :reconcile, st -> send(self(), :run_cycle) && st
      :noop, st -> st
    end)
  end

  # One RPC-confirmed reconcile cycle → a `{:cycle_result, …}` symbol for the
  # Machine. Fails closed on any RPC/apply error (`:deferred`).
  defp run_cycle(state) do
    case safe_rpc_height(state) do
      {:ok, node_height} ->
        local_before = state.local_height.()

        plan =
          Reconcile.reconcile_plan(
            local_before,
            node_height,
            state.local_hash_at,
            state.rpc_hash_at
          )

        case Reconcile.branch_for(plan, state.rpc_hash_at, state.batch) do
          :synced -> :synced
          :defer -> :deferred
          {:apply, arg} -> apply_outcome(state, arg, local_before)
        end

      :error ->
        :deferred
    end
  end

  # Dispatch the branch, then decide progress by **actual advancement**: a cycle is
  # `:progressed` (schedule a follow-up) only if the index moved forward; an apply
  # that errored, partially applied at the same height, or didn't advance is
  # `:deferred` (wait for the next trigger) — so a persistently-failing connect
  # block or a stuck node can never spin the controller.
  defp apply_outcome(state, arg, local_before) do
    _ = safe_apply(state, arg)
    if state.local_height.() > local_before, do: :progressed, else: :deferred
  end

  defp safe_apply(state, arg) do
    state.apply_fun.(state.processor, arg)
  rescue
    _ -> {:error, :apply_raised}
  catch
    :exit, _ -> {:error, :apply_exited}
  end

  # `RpcClient.get_block_count/0` (and any injected seam) may raise/exit on a
  # transient outage — treat that as "node height unknown" (fail closed).
  defp safe_rpc_height(state) do
    case state.rpc_height.() do
      {:ok, height} -> {:ok, height}
      _ -> :error
    end
  rescue
    _ -> :error
  catch
    :exit, _ -> :error
  end

  defp schedule_tick(interval_ms), do: Process.send_after(self(), :tick, interval_ms)

  ## ── Production seams ──

  defp default_rpc_height, do: RpcClient.get_block_count()

  defp default_rpc_hash_at(height) do
    case RpcClient.get_block_hash(height) do
      {:ok, hex} -> String.downcase(hex)
      _ -> nil
    end
  rescue
    _ -> nil
  catch
    :exit, _ -> nil
  end

  defp default_local_height do
    case BlockProcessContext |> order_by([b], desc: b.height) |> limit(1) |> Repo.one() do
      %BlockProcessContext{height: height} -> height
      nil -> 0
    end
  end

  defp default_local_hash_at(height) do
    case Repo.get_by(BlockProcessContext, height: height) do
      %BlockProcessContext{id: id} -> String.downcase(id)
      nil -> nil
    end
  end
end
