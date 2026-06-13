defmodule Athanor.P2P.HeadersChain do
  @moduledoc """
  Phase 6 T6.2 (§B) — the thin GenServer that turns the P2P frame stream into a
  best-tip-by-cumulative-work headers chain. It is a `frame_sink` member: the pool
  forwards each post-handshake frame as `{:peer, pid, :frame, %Frame{}}`, and this
  shell acts only on `inv(MSG_BLOCK)` and `headers`.

  All *decisions* live in the pure `HeadersChain.Tree`; this shell does IO:

    * **Seed once from REST.** On start it plants a synthetic root from the
      injected `:seed` seam (`-> {:ok, height, wire_hash} | {:error, _}`). A failed
      seed does not crash the chain — it starts inert and retries on `:tick`.
    * **`inv(MSG_BLOCK)` → `getheaders`.** A block inventory from a peer triggers a
      `getheaders` (locator from the `Tree`) back to that peer, de-duplicated per
      peer by a `:cooldown_ms` window.
    * **`headers` → connect.** A `headers` body is parsed (`Messages.Headers.parse/1`,
      already bounded; malformed/oversize dropped) and folded `{:connect, …}`
      through the `Tree`. `{:extend, …}`/`{:reorg, …}` are surfaced to the injected
      `:on_tip` sink with **display-order** hashes and reset the detached counter.
      A `{:detached, …}` run re-issues `getheaders` and increments the counter;
      after `:max_detached_rounds` consecutive detached rounds the fork is deeper
      than the window, so `:on_tip` is signalled `{:reorg_too_deep, …}` (operator
      alert + RPC fallback, §C) and the counter resets.
    * **`:tick`** → periodic `:prune` + an opportunistic `getheaders`.

  Fail-closed (Phase-5 consistency): every external call (`PeerRegistry.pids/1`,
  the `:seed`/`:on_tip` seams) is wrapped so a transient outage degrades rather
  than crashing the chain.

  Injected collaborators: `:seed`, `:on_tip`, `:registry`, `:selector`
  (`(pids -> peer)`), `:now_fun`, `:tick_interval_ms`, `:version`, `:cooldown_ms`,
  `:window`, `:max_detached_rounds`, `:pow_check` (forwarded to the `Tree`), and
  `:pow_limit` (consensus max-target compact; the default `:pow_check` enforces it
  via `Work.valid_pow?/3`, default `0x1d00ffff`).
  """

  use GenServer
  require Logger

  alias Athanor.P2P.Codec.Hash
  alias Athanor.P2P.{Frame, Peer, PeerRegistry}
  alias Athanor.P2P.HeadersChain.{Tree, Work}
  alias Athanor.P2P.Messages.{Headers, Inv}

  @max_inv_items 50_000

  # Consensus pow-limit (max target) compact when none is configured — the
  # mainnet/testnet value. The tree credits work only for headers at or below it.
  @default_pow_limit 0x1D00FFFF

  ## ── Client API ──

  @doc "Starts the headers chain. See the module doc for options."
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    {name, opts} = Keyword.pop(opts, :name)
    GenServer.start_link(__MODULE__, opts, if(name, do: [name: name], else: []))
  end

  ## ── Server callbacks ──

  @impl true
  def init(opts) do
    state = %{
      tree: nil,
      seed: Keyword.get(opts, :seed, fn -> {:error, :no_seed} end),
      on_tip: Keyword.get(opts, :on_tip, fn _ -> :ok end),
      registry: Keyword.get(opts, :registry, PeerRegistry),
      selector: Keyword.get(opts, :selector, &List.first/1),
      now_fun: Keyword.get(opts, :now_fun, fn -> System.monotonic_time(:millisecond) end),
      tick_interval_ms: Keyword.get(opts, :tick_interval_ms, 30_000),
      version: Keyword.get(opts, :version, 70_016),
      cooldown_ms: Keyword.get(opts, :cooldown_ms, 1_000),
      max_detached_rounds: Keyword.get(opts, :max_detached_rounds, 3),
      tree_opts: build_tree_opts(opts),
      detached_rounds: 0,
      detached_peer: nil,
      last_getheaders: %{}
    }

    schedule_tick(state.tick_interval_ms)
    {:ok, ensure_seeded(state)}
  end

  @impl true
  def handle_info({:peer, pid, :frame, %Frame{command: "inv", payload: payload}}, state) do
    state =
      if has_block_inv?(payload),
        do: send_getheaders(state, pid, false),
        else: state

    {:noreply, state}
  end

  def handle_info(
        {:peer, pid, :frame, %Frame{command: "headers", payload: payload}},
        %{tree: tree} = state
      )
      when not is_nil(tree) do
    case Headers.parse(payload) do
      {:ok, headers, _rest} when headers != [] ->
        {tree, events} = Tree.step(state.tree, {:connect, headers})
        {:noreply, apply_events(events, pid, %{state | tree: tree})}

      _ ->
        {:noreply, state}
    end
  end

  # Any other post-handshake frame (or headers before we are seeded) is ignored.
  def handle_info({:peer, _pid, :frame, %Frame{}}, state), do: {:noreply, state}

  def handle_info(:tick, state) do
    state = ensure_seeded(state)

    state =
      case state.tree do
        nil -> state
        tree -> opportunistic_getheaders(%{state | tree: elem(Tree.step(tree, :prune), 0)})
      end

    schedule_tick(state.tick_interval_ms)
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  ## ── Tip-event handling ──

  defp apply_events(events, source_pid, state) do
    # A batch that advanced the best tip is *progress* — a `{:detached, …}` carried
    # in the same batch (junk headers riding alongside real ones) must NOT count
    # against the deep-reorg counter (Hermes !18 note 941). Tip events also reset
    # the per-peer detached tracking entirely.
    progressed? =
      Enum.any?(events, &match?({:extend, _}, &1)) or Enum.any?(events, &match?({:reorg, _}, &1))

    Enum.reduce(events, state, fn event, st -> apply_event(event, source_pid, progressed?, st) end)
  end

  defp apply_event({:extend, hashes}, _source, _progressed?, state) do
    notify_tip(state, {:extend, display(hashes)})
    reset_detached(state)
  end

  defp apply_event({:reorg, %{orphan: orphan, connect: connect}}, _source, _progressed?, state) do
    notify_tip(state, {:reorg, %{orphan: display(orphan), connect: display(connect)}})
    reset_detached(state)
  end

  # Detached headers in a batch that also advanced the tip are ignored — progress
  # proves the chain is bridgeable, so this is not an unbridgeable-fork signal.
  defp apply_event({:detached, _count}, _source, true, state), do: state

  # Detached with no progress. The round counter is **scoped to a single peer**: a
  # detached run only escalates when the *same* peer keeps failing to bridge across
  # consecutive rounds, so junk from assorted peers can never accumulate to suspend
  # P2P authority (Hermes !18 note 941). A new peer restarts the count at 1.
  defp apply_event({:detached, _count}, source_pid, false, state) do
    rounds = if state.detached_peer == source_pid, do: state.detached_rounds + 1, else: 1

    if rounds >= state.max_detached_rounds do
      # The same peer's fork is deeper than our retained window: locator can't bridge it.
      Logger.warning(
        "HeadersChain: #{rounds} detached rounds from #{inspect(source_pid)} — escalating to deep-reorg fallback"
      )

      notify_tip(state, {:reorg_too_deep, %{rounds: rounds, peer: inspect(source_pid)}})
      reset_detached(state)
    else
      # Re-request from that peer (it claims to have the chain), tracking it as the
      # current fork candidate.
      %{
        send_getheaders(state, source_pid, true)
        | detached_rounds: rounds,
          detached_peer: source_pid
      }
    end
  end

  defp apply_event(_event, _source, _progressed?, state), do: state

  defp reset_detached(state), do: %{state | detached_rounds: 0, detached_peer: nil}

  ## ── getheaders ──

  defp send_getheaders(%{tree: nil} = state, _pid, _force), do: state

  defp send_getheaders(state, pid, force) do
    now = state.now_fun.()
    last = Map.get(state.last_getheaders, pid)

    if not force and is_integer(last) and now - last < state.cooldown_ms do
      state
    else
      {_tree, [{:locator, locator}]} = Tree.step(state.tree, {:locator, 32})

      Peer.send_frame(
        pid,
        :getheaders,
        Headers.serialize_get_headers(state.version, locator, <<0::256>>)
      )

      %{state | last_getheaders: Map.put(state.last_getheaders, pid, now)}
    end
  end

  defp opportunistic_getheaders(state) do
    case state.selector.(safe_pids(state.registry)) do
      nil -> state
      peer -> send_getheaders(state, peer, false)
    end
  end

  ## ── Private ──

  defp ensure_seeded(%{tree: nil} = state) do
    case safe_call(state.seed) do
      {:ok, height, hash} -> %{state | tree: Tree.new(hash, height, state.tree_opts)}
      _ -> state
    end
  end

  defp ensure_seeded(state), do: state

  defp has_block_inv?(payload) do
    case Inv.parse(payload, max_items: @max_inv_items) do
      {:ok, items, _rest} -> Enum.any?(items, &match?({:block, _}, &1))
      _ -> false
    end
  end

  defp notify_tip(state, event), do: safe_call(fn -> state.on_tip.(event) end)

  defp display(hashes), do: Enum.map(hashes, &Hash.wire_to_display/1)

  # `PeerRegistry.pids/1` is a GenServer.call — a registry that is absent/restarting
  # makes it exit; treat that as "no peers" (fail closed), mirroring Phase 5.
  defp safe_pids(registry) do
    PeerRegistry.pids(registry)
  rescue
    _ -> []
  catch
    :exit, _ -> []
  end

  # Run an injected 0-arity seam, normalizing any raise/exit to `:error` so a
  # transient outage degrades rather than crashing the chain.
  defp safe_call(fun) do
    fun.()
  rescue
    _ -> :error
  catch
    :exit, _ -> :error
  end

  defp schedule_tick(interval_ms), do: Process.send_after(self(), :tick, interval_ms)

  # Tree options. The PoW gate defaults to a **consensus pow-limit-aware** check
  # (`Work.valid_pow?/3` bound to `:pow_limit`, default mainnet/testnet
  # `0x1d00ffff`) so the production chain rejects easier-than-consensus headers; an
  # explicit `:pow_check` (e.g. a test bypass) still wins.
  defp build_tree_opts(opts) do
    pow_limit = Keyword.get(opts, :pow_limit, @default_pow_limit)
    default_check = fn hash, bits -> Work.valid_pow?(hash, bits, pow_limit) end

    opts
    |> Keyword.take([:window])
    |> Keyword.put(:pow_check, Keyword.get(opts, :pow_check, default_check))
  end
end
