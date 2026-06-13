defmodule Athanor.Workers.ChainTipVerifier do
  @moduledoc """
  Keeps the index consistent with the chain tip from two sources, with a single
  rollback/apply implementation underneath.

  * **P2P-primary (Phase 6 §C).** When peers are live, the pure `HeadersChain`
    decides the best tip by cumulative work and pushes `{:extend}`/`{:reorg}`/
    `{:reorg_too_deep}` events here via `apply_tip_event/1` (wired as its
    `:on_tip` sink). This is the `:chain_tip` route's `{:p2p, [:rpc]}` primary.
  * **RPC fallback.** A periodic poll (`verify_tip`) compares local height to the
    node and catches up / flags reorgs. It is the tip authority when P2P is not
    active (disabled or zero peers), and **defers** to P2P (passive consistency
    log only) when P2P is active — so there is never a double-driver.

  Both paths funnel into the existing `BlockProcessor` cast + `rollback_to/1`
  machinery; this module never duplicates that logic.
  """

  use GenServer
  require Logger

  alias Athanor.Repo
  alias Athanor.Schema.BlockProcessContext
  alias Athanor.Blockchain.RpcClient
  alias Athanor.Indexer.BlockProcessor
  alias Athanor.P2P.{HeadersChain, SourceRouter}
  import Ecto.Query

  @check_interval :timer.minutes(2)

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    schedule_check()
    {:ok, %{last_check: nil, consecutive_synced: 0, p2p_authority_suspended: false}}
  end

  @impl true
  def handle_info(:verify_tip, state) do
    Logger.debug("ChainTipVerifier: verifying chain tip")
    state = verify_chain_tip(state)
    schedule_check()
    {:noreply, %{state | last_check: DateTime.utc_now()}}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # Deep reorg: P2P cannot bridge it, so hand tip authority back to the RPC poll
  # even while peers stay live (otherwise `chain_tip_p2p_active?/0` would keep the
  # poll deferring and the index could stall). Cleared when a healthy P2P tip
  # event arrives (recovery).
  @impl true
  def handle_cast({:suspend_p2p_authority, info}, state) do
    Logger.warning(
      "ChainTipVerifier: suspending P2P tip authority (#{inspect(info)}); RPC poll resumes"
    )

    {:noreply, %{state | p2p_authority_suspended: true}}
  end

  def handle_cast(:resume_p2p_authority, %{p2p_authority_suspended: true} = state) do
    Logger.info("ChainTipVerifier: P2P tip recovered; resuming P2P tip authority")
    {:noreply, %{state | p2p_authority_suspended: false}}
  end

  def handle_cast(:resume_p2p_authority, state), do: {:noreply, state}

  ## ── P2P tip-event bridge (§C) ──

  @doc """
  Applies a tip event emitted by the P2P `HeadersChain` to the index.

  This is the bridge from the pure header-chain decision to the existing block
  machinery — it is wired as the chain's `:on_tip` sink (`&apply_tip_event/1`).
  Event hashes are **display order** (as `HeadersChain` emits them).

    * `{:extend, hashes}` — new best-chain blocks (ancestor→tip) are enqueued to
      `BlockProcessor` for normal confirmation; P2P authority is (re)confirmed.
    * `{:reorg, %{orphan: orphan, connect: connect}}` — the common-ancestor height
      is resolved (one below the lowest orphan height, from `block_process_contexts`,
      or `nil` if none on record) and rollback + the new branch (`connect`,
      ancestor→tip) are dispatched as **one ordered `BlockProcessor.apply_reorg/3`
      mailbox op**, so rollback can never race in-flight block casts. P2P authority
      is (re)confirmed.
    * `{:reorg_too_deep, info}` — the fork is deeper than the retained header
      window, so P2P cannot bridge it: P2P tip authority is **suspended** (a cast
      to this verifier) so the RPC poll resumes catch-up even while peers stay live,
      until a healthy P2P tip event recovers it.

  ## Parameters
    - `event` — the tip event tuple from `HeadersChain`.
    - `opts` (injection seams, for tests): `:processor` (BlockProcessor name/pid,
      default `BlockProcessor`), `:verifier` (this GenServer's name/pid for the
      suspend/resume casts, default `__MODULE__`), `:resolve_height`
      (`(orphan_hashes -> {:ok, height} | :unknown)`, default the Repo lookup).

  ## Returns
    `:ok`.
  """
  @spec apply_tip_event(tuple()) :: :ok
  def apply_tip_event(event), do: apply_tip_event(event, [])

  @spec apply_tip_event(tuple(), keyword()) :: :ok
  def apply_tip_event({:extend, hashes}, opts) do
    resume_authority(opts)
    enqueue_blocks(hashes, opts)
  end

  def apply_tip_event({:reorg, %{orphan: orphan, connect: connect}}, opts) do
    fork_height =
      case resolve_fork_height(orphan, opts) do
        {:ok, height} -> height
        :unknown -> nil
      end

    Logger.warning(
      "ChainTipVerifier: P2P reorg — rollback to #{inspect(fork_height)} + #{length(connect)} " <>
        "block(s) as one ordered BlockProcessor op"
    )

    resume_authority(opts)
    processor = Keyword.get(opts, :processor, BlockProcessor)
    BlockProcessor.apply_reorg(processor, fork_height, connect)
    :ok
  end

  def apply_tip_event({:reorg_too_deep, info}, opts) do
    Logger.warning(
      "ChainTipVerifier: deep reorg signalled (#{inspect(info)}) — beyond the header " <>
        "window; suspending P2P tip authority so RPC takes over"
    )

    GenServer.cast(Keyword.get(opts, :verifier, __MODULE__), {:suspend_p2p_authority, info})
    :ok
  end

  def apply_tip_event(_other, _opts), do: :ok

  @doc """
  Whether the P2P `HeadersChain` is the **active** tip authority right now — i.e.
  the `:chain_tip` route's **primary** provider is `:p2p` *and* P2P is available.

  The primary check is explicit (`SourceRouter.resolve/1`): `SourceRouter.route/3`
  would fall through to a `:p2p` *fallback* once peers are live, so an operator
  override like `chain_tip: {:rpc, [:p2p]}` (RPC-primary, P2P fallback) must NOT
  read as P2P-active — otherwise the RPC-primary override is silently disabled
  (Hermes !18 note 963 B1). Availability reuses `SourceRouter`'s exact gate (one
  definition of "P2P is up"). When false, the RPC poll is the authority.

  `opts` accepts `:p2p_available?` (a boolean or 0-arity fun), forwarded to
  `SourceRouter.route/3`.
  """
  @spec chain_tip_p2p_active?(keyword()) :: boolean()
  def chain_tip_p2p_active?(opts \\ []) do
    with {:p2p, _fallbacks} <- SourceRouter.resolve(:chain_tip),
         {:ok, :p2p} <- SourceRouter.route(:chain_tip, &chain_tip_p2p_attempt/1, opts) do
      true
    else
      _ -> false
    end
  end

  defp chain_tip_p2p_attempt(:p2p), do: {:ok, :p2p}
  defp chain_tip_p2p_attempt(_provider), do: :miss

  @doc """
  Whether the RPC poll should defer active catch-up to P2P: only when P2P is the
  live tip authority (`chain_tip_p2p_active?/1`) **and** it is not currently
  suspended by a deep-reorg signal. While suspended, the RPC poll takes over even
  though peers remain live (blocker 2).
  """
  @spec should_defer_to_p2p?(boolean(), keyword()) :: boolean()
  def should_defer_to_p2p?(suspended?, opts \\ []) do
    not suspended? and chain_tip_p2p_active?(opts)
  end

  @doc """
  Whether the RPC poll should hand tip authority to P2P this cycle. In addition to
  `should_defer_to_p2p?/2` (P2P live + not suspended), the local index must have
  **caught up to the P2P seed/root height** (`p2p_root_height`) — otherwise there is
  a local→seed gap that only the RPC catch-up path fills, and deferring would let
  P2P extend above an unprocessed gap (Hermes !18 note 945 B1). `nil`
  `p2p_root_height` (chain unseeded) → never defer.
  """
  @spec defer_to_p2p?(boolean(), non_neg_integer(), non_neg_integer() | nil, keyword()) ::
          boolean()
  def defer_to_p2p?(suspended?, local_height, p2p_root_height, opts \\ []) do
    should_defer_to_p2p?(suspended?, opts) and caught_up_to_seed?(local_height, p2p_root_height)
  end

  defp caught_up_to_seed?(_local_height, nil), do: false
  defp caught_up_to_seed?(local_height, root_height), do: local_height >= root_height

  ## ── Private ──

  defp enqueue_blocks(hashes, opts) do
    processor = Keyword.get(opts, :processor, BlockProcessor)
    Enum.each(hashes, fn hash -> GenServer.cast(processor, {:process_block_hash, hash}) end)
    :ok
  end

  # A healthy P2P tip event means P2P recovered — clear any deep-reorg suspension.
  defp resume_authority(opts) do
    GenServer.cast(Keyword.get(opts, :verifier, __MODULE__), :resume_p2p_authority)
  end

  defp resolve_fork_height(orphan, opts) do
    case Keyword.get(opts, :resolve_height) do
      fun when is_function(fun, 1) -> fun.(orphan)
      nil -> fork_height_from_db(orphan)
    end
  end

  # The orphaned blocks were our previous best chain, so they are recorded in
  # `block_process_contexts` (keyed by display-order hash hex). The common-ancestor
  # (fork) height is one below the lowest orphan height. Returns `:unknown` when
  # none are on record (we never processed that branch).
  defp fork_height_from_db(orphan_display_hashes) do
    hexes = Enum.map(orphan_display_hashes, &Base.encode16(&1, case: :lower))

    heights =
      BlockProcessContext
      |> where([b], b.id in ^hexes)
      |> select([b], b.height)
      |> Repo.all()

    case heights do
      [] -> :unknown
      hs -> {:ok, Enum.min(hs) - 1}
    end
  end

  defp schedule_check do
    Process.send_after(self(), :verify_tip, @check_interval)
  end

  defp verify_chain_tip(state) do
    with {:ok, node_height} <- RpcClient.get_block_count() do
      local_height = local_tip_height()

      if defer_to_p2p?(state.p2p_authority_suspended, local_height, p2p_root_height()) do
        # P2P is the active tip authority and the local index has reached the P2P
        # seed, so P2P is already driving catch-up via `apply_tip_event/1`; the RPC
        # poll stays a passive consistency check.
        Logger.debug("ChainTipVerifier: P2P is the active tip authority; RPC poll passive")
        %{state | consecutive_synced: 0}
      else
        # RPC is the authority (P2P disabled / zero peers / deep-reorg suspended).
        # Reconcile by HASH from the common ancestor — same-height divergence and
        # orphaned tips must be detected and recovered, not read as synced.
        rpc_reconcile(local_height, node_height, state)
      end
    else
      {:error, reason} ->
        Logger.warning("ChainTipVerifier: failed to verify tip: #{inspect(reason)}")
        state
    end
  end

  # Run a reconciliation cycle with RPC as the authority, then fold the outcome
  # into the synced counter. The plan itself `:defer`s on any unproven ancestor or
  # missing canonical hash, so a transient RPC hiccup / pruned hash never triggers
  # a spurious rollback — it simply retries on the next tick.
  defp rpc_reconcile(local_height, node_height, state) do
    apply_reconcile_result(reconcile(local_height, node_height, default_reconcile_opts()), state)
  end

  defp apply_reconcile_result(:defer, state), do: state

  defp apply_reconcile_result(:synced, state),
    do: %{state | consecutive_synced: state.consecutive_synced + 1}

  defp apply_reconcile_result(_acted, state), do: %{state | consecutive_synced: 0}

  @doc """
  Plans RPC-authority reconciliation by **hash** (Hermes !18 notes 937, 941). Walks
  down from `min(local_height, node_height)` looking for the highest height where
  the local and node hashes are both known **and** agree (the common ancestor):

    * `:synced` — heights equal and tips agree;
    * `{:catch_up, from, to}` — no divergence, the node is simply ahead;
    * `{:reorg, ancestor, to}` — the chains diverge: roll back to `ancestor` and
      reprocess the canonical branch from `ancestor + 1` up to `to`;
    * `:defer` — a local or node hash below the tip is **unknown**, so the ancestor
      cannot be positively proven this cycle. Never reorg/rollback on an unproven
      ancestor (a transient/pruned hash must not look like a real mismatch).

  `local_hash_at`/`node_hash_at` are `(height -> hash | nil)`. Pure.
  """
  @spec reconcile_plan(non_neg_integer(), non_neg_integer(), fun(), fun()) ::
          :synced
          | :defer
          | {:catch_up, pos_integer(), non_neg_integer()}
          | {:reorg, non_neg_integer(), non_neg_integer()}
  def reconcile_plan(0, node_height, _local_hash_at, _node_hash_at),
    do: {:catch_up, 1, node_height}

  def reconcile_plan(local_height, node_height, local_hash_at, node_hash_at) do
    case find_ancestor(min(local_height, node_height), local_hash_at, node_hash_at) do
      :defer ->
        :defer

      {:ok, ancestor} ->
        cond do
          ancestor == local_height and local_height == node_height -> :synced
          ancestor == local_height -> {:catch_up, local_height + 1, node_height}
          true -> {:reorg, ancestor, node_height}
        end
    end
  end

  # Highest height (≤ `h`) at which the local and node hashes are both known and
  # equal. Returns `:defer` the moment either hash is unknown before a match is
  # found — an unknown hash is NOT a mismatch, so we never walk past it into a
  # destructive deep rollback without positively proving the ancestor.
  defp find_ancestor(h, _local_hash_at, _node_hash_at) when h < 0, do: :defer

  defp find_ancestor(h, local_hash_at, node_hash_at) do
    lh = local_hash_at.(h)
    nh = node_hash_at.(h)

    cond do
      is_nil(lh) or is_nil(nh) -> :defer
      lh == nh -> {:ok, h}
      true -> find_ancestor(h - 1, local_hash_at, node_hash_at)
    end
  end

  @doc """
  Executes a `reconcile_plan/4` against the node, dispatching the recovery to
  `BlockProcessor`. A `{:reorg, …}` is a single ordered `apply_reorg/3` op
  (rollback + the **contiguous** canonical prefix from `ancestor + 1`); a
  `{:catch_up, …}` enqueues the contiguous forward prefix. Stopping at the first
  missing/invalid hash (rather than skipping it) guarantees no gap is recorded —
  the cycle `:defer`s if the required first block is unavailable. Returns the plan
  executed, or `:defer`. `opts`: `:local_hash_at`, `:node_hash_at`
  (`(height -> hash | nil)`), `:processor`, `:batch` (max blocks/cycle, default 10).
  """
  @spec reconcile(non_neg_integer(), non_neg_integer(), keyword()) :: term()
  def reconcile(local_height, node_height, opts) do
    local_hash_at = Keyword.fetch!(opts, :local_hash_at)
    node_hash_at = Keyword.fetch!(opts, :node_hash_at)
    processor = Keyword.get(opts, :processor, BlockProcessor)
    batch = Keyword.get(opts, :batch, 10)

    case reconcile_plan(local_height, node_height, local_hash_at, node_hash_at) do
      :defer ->
        :defer

      :synced ->
        :synced

      {:catch_up, from, to} = plan ->
        case branch_hashes(from, to, node_hash_at, batch) do
          # The first canonical block is unavailable — defer rather than record a gap.
          [] ->
            :defer

          hashes ->
            Enum.each(hashes, &GenServer.cast(processor, {:process_block_hash, &1}))
            plan
        end

      {:reorg, ancestor, to} = plan ->
        do_reorg(processor, ancestor, to, node_hash_at, batch, plan)
    end
  end

  # `to <= ancestor`: the node is at/below the common ancestor — the local tip is an
  # orphan fork above it. Roll back to the ancestor; there is no canonical branch to
  # connect. Otherwise require the canonical block at `ancestor + 1` to be available
  # (a contiguous prefix) before rolling back, so we never roll back into a gap.
  defp do_reorg(processor, ancestor, to, _node_hash_at, _batch, plan) when to <= ancestor do
    BlockProcessor.apply_reorg(processor, ancestor, [])
    plan
  end

  defp do_reorg(processor, ancestor, to, node_hash_at, batch, plan) do
    case branch_hashes(ancestor + 1, to, node_hash_at, batch) do
      [] ->
        :defer

      connect ->
        BlockProcessor.apply_reorg(processor, ancestor, connect)
        plan
    end
  end

  # The **contiguous** decoded node block-hash prefix for `from..to`, capped to
  # `batch` blocks per cycle. Stops at the first missing/invalid hash (returning the
  # prefix only) so a gap is never silently bridged — the remainder is picked up by
  # later ticks once the node has it.
  defp branch_hashes(from, to, _node_hash_at, _batch) when from > to, do: []

  defp branch_hashes(from, to, node_hash_at, batch) do
    contiguous_hashes(from, min(from + batch - 1, to), node_hash_at, [])
  end

  defp contiguous_hashes(height, last, _node_hash_at, acc) when height > last,
    do: Enum.reverse(acc)

  defp contiguous_hashes(height, last, node_hash_at, acc) do
    case decode_hash(node_hash_at.(height)) do
      nil -> Enum.reverse(acc)
      hash -> contiguous_hashes(height + 1, last, node_hash_at, [hash | acc])
    end
  end

  defp decode_hash(nil), do: nil

  defp decode_hash(hex) do
    case Base.decode16(hex, case: :mixed) do
      {:ok, binary} -> binary
      :error -> nil
    end
  end

  # Production reconcile seams: local hashes from `block_process_contexts`, node
  # hashes from RPC. Hashes are compared as lowercase hex so the two sources line
  # up (the context id is `encode16(:lower)`; RPC hex case may differ).
  defp default_reconcile_opts do
    [
      local_hash_at: &local_hash_at/1,
      node_hash_at: &rpc_hash_at/1,
      processor: BlockProcessor
    ]
  end

  defp local_tip_height do
    case BlockProcessContext |> order_by([b], desc: b.height) |> limit(1) |> Repo.one() do
      %BlockProcessContext{height: height} -> height
      nil -> 0
    end
  end

  defp local_hash_at(height) do
    case Repo.get_by(BlockProcessContext, height: height) do
      %BlockProcessContext{id: id} -> String.downcase(id)
      nil -> nil
    end
  end

  defp rpc_hash_at(height) do
    case RpcClient.get_block_hash(height) do
      {:ok, hex} -> String.downcase(hex)
      {:error, _reason} -> nil
    end
  end

  # The P2P chain's seed/root height, or `nil` if unseeded/unavailable. Wrapped
  # fail-closed (Phase-5 consistency): a `HeadersChain` that is absent/restarting
  # makes the call exit; treat that as "no P2P window" so the poll keeps RPC
  # authority rather than crashing or wrongly deferring.
  defp p2p_root_height do
    HeadersChain.root_height()
  rescue
    _ -> nil
  catch
    :exit, _ -> nil
  end
end
