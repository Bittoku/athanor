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
  alias Athanor.P2P.SourceRouter
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
  the `:chain_tip` route's primary is `:p2p` *and* P2P is available. Reuses
  `SourceRouter`'s exact availability gate (so there is one definition of
  "P2P is up"). When false, the RPC poll is the authority (cold-start parity).

  `opts` accepts `:p2p_available?` (a boolean or 0-arity fun), forwarded to
  `SourceRouter.route/3`.
  """
  @spec chain_tip_p2p_active?(keyword()) :: boolean()
  def chain_tip_p2p_active?(opts \\ []) do
    match?(
      {:ok, :p2p},
      SourceRouter.route(
        :chain_tip,
        fn
          :p2p -> {:ok, :p2p}
          _ -> :miss
        end,
        opts
      )
    )
  end

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
    with {:ok, node_height} <- RpcClient.get_block_count(),
         {:ok, _node_hash} <- RpcClient.get_block_hash(node_height) do
      local_tip =
        BlockProcessContext
        |> order_by([b], desc: b.height)
        |> limit(1)
        |> Repo.one()

      local_height = if local_tip, do: local_tip.height, else: 0

      cond do
        local_height == node_height ->
          # In sync
          %{state | consecutive_synced: state.consecutive_synced + 1}

        local_height < node_height ->
          # Behind. When the P2P HeadersChain is the active tip authority (and not
          # suspended by a deep reorg) it is already driving catch-up via
          # `apply_tip_event/1`, so the RPC poll only logs (passive consistency
          # check) and does not double-drive. When P2P is not active (disabled /
          # zero peers) or suspended, the RPC poll catches up — cold-start parity
          # and the deep-reorg fallback.
          if should_defer_to_p2p?(state.p2p_authority_suspended) do
            Logger.debug(
              "ChainTipVerifier: #{node_height - local_height} behind; P2P is active tip authority, deferring catch-up"
            )
          else
            Logger.info(
              "ChainTipVerifier: #{node_height - local_height} blocks behind, catching up"
            )

            catch_up(local_height + 1, node_height)
          end

          %{state | consecutive_synced: 0}

        local_height > node_height ->
          # Ahead of node? Possible reorg
          Logger.warning(
            "ChainTipVerifier: local height #{local_height} > node #{node_height}, possible reorg"
          )

          %{state | consecutive_synced: 0}
      end
    else
      {:error, reason} ->
        Logger.warning("ChainTipVerifier: failed to verify tip: #{inspect(reason)}")
        state
    end
  end

  defp catch_up(from_height, to_height) when from_height > to_height, do: :ok

  defp catch_up(from_height, to_height) do
    # Process up to 10 blocks per cycle to avoid blocking
    max_height = min(from_height + 9, to_height)

    Enum.each(from_height..max_height, fn height ->
      case RpcClient.get_block_hash(height) do
        {:ok, hash_hex} ->
          case Base.decode16(hash_hex, case: :mixed) do
            {:ok, hash_binary} ->
              GenServer.cast(BlockProcessor, {:process_block_hash, hash_binary})

            :error ->
              Logger.warning("ChainTipVerifier: invalid block hash at height #{height}")
          end

        {:error, reason} ->
          Logger.warning("ChainTipVerifier: failed to get hash for #{height}: #{inspect(reason)}")
      end
    end)
  end
end
