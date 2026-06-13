defmodule Athanor.P2P.TxRelay do
  @moduledoc """
  Phase 4 T4.1 — the thin GenServer that broadcasts our own transactions to the
  peer set and confirms their propagation. It is a `frame_sink` member (§A): the
  pool fans each post-handshake application frame out to it as
  `{:peer, pid, :frame, %Frame{}}`, carrying the originating peer pid.

  All *decisions* live in the pure relay `Tracker` (§B); this shell only does IO:
  it folds inbound frames through the `Tracker` and performs the emitted actions.

  ## Outbound entry point

    * `broadcast(server, txid, raw_bin)` — a **synchronous** `GenServer.call`
      carrying the already-validated decoded binary tx bytes and the derived
      `txid` (validation is upstream in `broadcast_tx/2`, §C; the relay is
      fire-and-forget and never returns a parse error). It rechecks
      `PeerRegistry.pids/1` and:
        * returns `{:error, :no_peers}` if the registry now reads **zero** peers
          (peer-churn race) — **before** computing `held`, so `held = min(2, N−1)`
          is only evaluated for `N ≥ 1` and can never go negative;
        * otherwise picks the announce/hold-back split through the injected
          `:selector` seam (`select(pids, held) -> {targets, held_back}`; default
          random shuffle, deterministic in tests), steps the `Tracker` with the
          normative `{:broadcast, txid, raw_bin, targets, bar}` 5-tuple
          (`bar = held = min(2, N−1)`), announces `inv({:tx, txid})` to each
          target, and returns `:ok` — or `{:error, :saturated}` at the
          `max_pending` cap.
      It is synchronous only so the saturated / no-peers verdict is available to
      `broadcast_tx/2` immediately; it does **not** wait on propagation.

  ## Inbound frames (folded through the `Tracker`)

    * `getdata` → serve the stored `raw_bin` (binary wire bytes) to the
      requesting peer via `Peer.send_frame(peer, :tx, raw_bin)`, once per
      `(txid, peer)`.
    * `inv` → a relay-back from a peer **not** in `announced_to` is propagation
      evidence; at `bar` distinct non-targets the `:propagated` audit fires once.
    * `reject` (`message: "tx"`, with the 32-byte txid in `data`) → surface the
      peer's refusal to the audit sink.
    * `:tick` → expire stale pending as `:unconfirmed` (audited) and drop it.

  Inbound `inv`/`getdata` bodies are parsed with the Phase-3 `Inv.parse`
  `:max_items` guard; an over-large or malformed body is dropped before the
  reducer is ever called (no unbounded parse work on unsolicited frames).

  ## Injected collaborators (testability)

    * `:registry` — the `PeerRegistry` server (default `PeerRegistry`).
    * `:selector` — `(pids, held) -> {targets, held_back}` (default: random
      shuffle so the announce/hold-back split is not a stable per-node
      fingerprint; tests inject a deterministic split).
    * `:audit` — `(event -> any)` sink for `{:propagated, txid}` /
      `{:rejected, txid, peer, reason}` / `{:unconfirmed, txid}` (default: log).
    * `:tracker` — a `Tracker` struct or keyword opts (default `Tracker.new/1`;
      `:max_pending`/`:ttl_ms` may also be passed at the top level).
    * `:now_fun` — `-> integer ms` (default `System.monotonic_time(:millisecond)`).
    * `:tick_interval_ms` — TTL sweep interval (default 1_000).
  """

  use GenServer
  require Logger

  alias Athanor.P2P.{Frame, Peer, PeerRegistry}
  alias Athanor.P2P.Messages.{Inv, Reject}
  alias Athanor.P2P.TxRelay.Tracker

  # CompactSize guard for inbound inventory vectors (malicious-peer bound),
  # matching the Phase-3 mempool observer.
  @max_inv_items 50_000

  ## ── Client API ──

  @doc """
  Starts the relay.

  ## Options
  See the module doc — `:name`, `:registry`, `:selector`, `:audit`, `:tracker`
  (or `:max_pending`/`:ttl_ms`), `:now_fun`, `:tick_interval_ms`.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    {name, opts} = Keyword.pop(opts, :name)
    GenServer.start_link(__MODULE__, opts, if(name, do: [name: name], else: []))
  end

  @doc """
  Synchronously enqueues a broadcast of `raw_bin` (decoded binary tx bytes) under
  `txid` (wire/internal order). Returns `:ok` once announced, `{:error,
  :saturated}` at the `max_pending` cap, or `{:error, :no_peers}` if the live
  peer set is now empty. See the module doc for the full contract.
  """
  @spec broadcast(GenServer.server(), Tracker.txid(), binary()) ::
          :ok | {:error, :saturated} | {:error, :no_peers}
  def broadcast(server \\ __MODULE__, txid, raw_bin),
    do: GenServer.call(server, {:broadcast, txid, raw_bin})

  ## ── Server callbacks ──

  @impl true
  def init(opts) do
    tracker =
      case Keyword.get(opts, :tracker) do
        %Tracker{} = t -> t
        o when is_list(o) -> Tracker.new(o)
        nil -> Tracker.new(Keyword.take(opts, [:max_pending, :ttl_ms]))
      end

    state = %{
      tracker: tracker,
      registry: Keyword.get(opts, :registry, PeerRegistry),
      selector: Keyword.get(opts, :selector, &default_selector/2),
      audit: Keyword.get(opts, :audit, &default_audit/1),
      now_fun: Keyword.get(opts, :now_fun, fn -> System.monotonic_time(:millisecond) end),
      tick_interval_ms: Keyword.get(opts, :tick_interval_ms, 1_000)
    }

    schedule_tick(state.tick_interval_ms)
    {:ok, state}
  end

  @impl true
  def handle_call({:broadcast, txid, raw_bin}, _from, state) do
    # Recheck the live-peer set here (peer-churn race): zero peers means P2P
    # can't carry the tx, so signal :no_peers BEFORE computing `held` — that
    # keeps `held = min(2, N−1)` out of the N == 0 case entirely. The recheck is
    # itself fail-safe: a registry that is absent/restarting makes `pids/1` exit,
    # which we normalize to "no peers" so the broadcaster (RPC) fallback runs.
    case safe_pids(state.registry) do
      [] ->
        {:reply, {:error, :no_peers}, state}

      pids ->
        held = min(2, length(pids) - 1)
        {targets, _held_back} = state.selector.(pids, held)

        {tracker, actions} =
          Tracker.step(state.tracker, {:broadcast, txid, raw_bin, targets, held}, now(state))

        apply_actions(actions, state)
        reply = if saturated?(actions), do: {:error, :saturated}, else: :ok
        {:reply, reply, %{state | tracker: tracker}}
    end
  end

  @impl true
  def handle_info({:peer, pid, :frame, %Frame{command: "getdata", payload: payload}}, state),
    do: fold_invs_and_apply(payload, pid, :getdata, state)

  def handle_info({:peer, pid, :frame, %Frame{command: "inv", payload: payload}}, state),
    do: fold_invs_and_apply(payload, pid, :inv, state)

  def handle_info({:peer, pid, :frame, %Frame{command: "reject", payload: payload}}, state) do
    case Reject.parse(payload) do
      {:ok, %Reject{message: "tx", data: <<txid::binary-32>>, ccode: ccode, reason: reason}, _} ->
        {tracker, actions} =
          Tracker.step(state.tracker, {:reject, txid, pid, "#{ccode}: #{reason}"}, now(state))

        apply_actions(actions, state)
        {:noreply, %{state | tracker: tracker}}

      _ ->
        {:noreply, state}
    end
  end

  # Any other post-handshake application frame is not the relay's concern.
  def handle_info({:peer, _pid, :frame, %Frame{}}, state), do: {:noreply, state}

  def handle_info(:tick, state) do
    {tracker, actions} = Tracker.step(state.tracker, :tick, now(state))
    apply_actions(actions, state)
    schedule_tick(state.tick_interval_ms)
    {:noreply, %{state | tracker: tracker}}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  ## ── Private ──

  # Fold every advertised tx in an inv/getdata body through the Tracker (the
  # `:max_items` guard drops an over-large/malformed body before the reducer),
  # then perform the emitted actions.
  defp fold_invs_and_apply(payload, peer, kind, state) do
    now = now(state)

    {tracker, actions} =
      payload
      |> tx_hashes()
      |> Enum.reduce({state.tracker, []}, fn txid, {tracker, acc} ->
        {tracker, new} = Tracker.step(tracker, event(kind, txid, peer), now)
        {tracker, acc ++ new}
      end)

    apply_actions(actions, state)
    {:noreply, %{state | tracker: tracker}}
  end

  defp event(:getdata, txid, peer), do: {:getdata, txid, peer}
  defp event(:inv, txid, peer), do: {:inv, txid, peer}

  defp apply_actions(actions, state), do: Enum.each(actions, &apply_action(&1, state))

  defp apply_action({:send_inv, peer, txid}, _state),
    do: Peer.send_frame(peer, :inv, Inv.serialize([{:tx, txid}]))

  defp apply_action({:send_tx, peer, raw}, _state), do: Peer.send_frame(peer, :tx, raw)
  defp apply_action({:propagated, txid}, state), do: state.audit.({:propagated, txid})

  defp apply_action({:rejected, txid, peer, reason}, state),
    do: state.audit.({:rejected, txid, peer, reason})

  defp apply_action({:unconfirmed, txid}, state), do: state.audit.({:unconfirmed, txid})
  # `:saturated` is surfaced synchronously via the call reply, not as an action.
  defp apply_action({:saturated, _txid}, _state), do: :ok

  defp saturated?(actions), do: Enum.any?(actions, &match?({:saturated, _}, &1))

  defp tx_hashes(payload) do
    case Inv.parse(payload, max_items: @max_inv_items) do
      {:ok, items, _rest} -> for {:tx, hash} <- items, do: hash
      _ -> []
    end
  end

  # Default production selector: randomly split so the announce/hold-back pattern
  # is not a stable per-node origin fingerprint. The first `held` shuffled pids
  # are held back; the rest are the announce targets.
  defp default_selector(pids, held) do
    {held_back, targets} = pids |> Enum.shuffle() |> Enum.split(held)
    {targets, held_back}
  end

  defp default_audit(event), do: Logger.debug("TxRelay audit: #{inspect(event)}")

  # `PeerRegistry.pids/1` is a `GenServer.call` — a registry that is absent or
  # restarting makes it exit. Treat that as "no peers" so the broadcast degrades
  # to the RPC fallback rather than crashing the relay (and its caller).
  defp safe_pids(registry) do
    PeerRegistry.pids(registry)
  rescue
    _ -> []
  catch
    :exit, _ -> []
  end

  defp now(state), do: state.now_fun.()

  defp schedule_tick(interval_ms), do: Process.send_after(self(), :tick, interval_ms)
end
