defmodule Athanor.P2P.MempoolObserver do
  @moduledoc """
  Phase 3 T3.3 — the thin GenServer that turns the P2P frame stream into indexed
  mempool transactions. It is the pool's `frame_sink` (§C): the pool forwards
  each post-handshake application frame as `{:peer, pid, :frame, %Frame{}}`,
  carrying the originating peer pid.

  Responsibilities (all *decisions* live in the pure `Tracker`, §B):

    * `inv` → for each advertised `MSG_TX`, fold `{:inv, txid, peer}` through the
      `Tracker`; a `{:getdata, peer, txid}` action is written back to **that**
      peer via `Peer.send_frame/3` (§C), and the peer is monitored so its exit
      becomes a `Tracker` `:peer_down`.
    * `tx` → re-hash the payload to recover the txid (forgery guard: a payload
      whose hash isn't an `outstanding` request is ignored by the `Tracker`),
      then on the `{:ingest, _}` action run the `Watchlist` prefilter →
      `matcher` (`TransactionFilter.matches?/1`) → `pipeline` with `source:
      :p2p`.
    * `notfound` → clear the `Tracker`'s outstanding entry (re-requestable).
    * `:tick` / per-request `:request_timeout` / monitor `:DOWN` → drive the
      `Tracker`'s refill/expiry/peer-down transitions.

  Collaborators are injected for testability: `:watchlist` (an `:ets` table),
  `:matcher` (`tx -> {addresses, tokens}`), `:pipeline`
  (`tx, addresses, tokens, source -> any`), `:now_fun` (`-> integer ms`), and
  the `:tick_interval_ms` / `:request_timeout_ms` timer intervals. Time and
  timers are injected so unit tests need no wall-clock or sleeps.
  """

  use GenServer
  require Logger

  alias Athanor.Indexer.{TransactionFilter, TransactionProcessor}
  alias Athanor.P2P.{Frame, Peer, Watchlist}
  alias Athanor.P2P.Messages.Inv
  alias Athanor.P2P.MempoolObserver.Tracker

  # CompactSize guard for inbound inventory vectors (malicious-peer bound).
  @max_inv_items 50_000

  ## ── Client API ──

  @doc """
  Starts the observer.

  ## Options
    * `:name` — optional registered name (the pool's `frame_sink` is a pid).
    * `:watchlist` — `Watchlist` `:ets` table (default: a fresh one).
    * `:matcher` — `tx -> {addresses, tokens}` (default `TransactionFilter.matches?/1`).
    * `:pipeline` — `tx, addresses, tokens, source -> any` (default: index via
      `TransactionProcessor.process_tx/4`).
    * `:now_fun` — `-> integer` monotonic ms (default `System.monotonic_time/1`).
    * `:tracker` — a `Tracker` struct or keyword opts (default `Tracker.new/1`).
    * `:tick_interval_ms` / `:request_timeout_ms` — timer intervals.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    {name, opts} = Keyword.pop(opts, :name)
    GenServer.start_link(__MODULE__, opts, if(name, do: [name: name], else: []))
  end

  ## ── Server callbacks ──

  @impl true
  def init(opts) do
    request_timeout_ms = Keyword.get(opts, :request_timeout_ms, 30_000)

    tracker =
      case Keyword.get(opts, :tracker) do
        %Tracker{} = t -> t
        opts when is_list(opts) -> Tracker.new(opts)
        nil -> Tracker.new(request_timeout_ms: request_timeout_ms)
      end

    state = %{
      tracker: tracker,
      watchlist: Keyword.get(opts, :watchlist) || Watchlist.new(),
      matcher: Keyword.get(opts, :matcher, &TransactionFilter.matches?/1),
      pipeline: Keyword.get(opts, :pipeline, &default_pipeline/4),
      now_fun: Keyword.get(opts, :now_fun, fn -> System.monotonic_time(:millisecond) end),
      tick_interval_ms: Keyword.get(opts, :tick_interval_ms, 1_000),
      request_timeout_ms: request_timeout_ms,
      monitors: %{}
    }

    schedule_tick(state.tick_interval_ms)
    {:ok, state}
  end

  @impl true
  def handle_info({:peer, pid, :frame, %Frame{command: "inv", payload: payload}}, state) do
    now = state.now_fun.()

    {tracker, actions} =
      payload
      |> tx_hashes()
      |> Enum.reduce({state.tracker, []}, fn txid, {tracker, acc} ->
        {tracker, new_actions} = Tracker.step(tracker, {:inv, txid, pid}, now)
        {tracker, acc ++ new_actions}
      end)

    state = Enum.reduce(actions, %{state | tracker: tracker}, &apply_action/2)
    {:noreply, state}
  end

  def handle_info({:peer, pid, :frame, %Frame{command: "tx", payload: payload}}, state) do
    case BSV.Transaction.from_binary(payload) do
      {:ok, tx, _rest} ->
        # Re-hash to recover the txid the Tracker keys on; a payload whose hash
        # isn't an outstanding request yields no `:ingest` action (forgery guard).
        txid = BSV.Transaction.txid_binary(tx)

        {tracker, actions} =
          Tracker.step(state.tracker, {:tx, txid, payload, pid}, state.now_fun.())

        if Enum.any?(actions, &match?({:ingest, _}, &1)), do: ingest(tx, state)
        {:noreply, %{state | tracker: tracker}}

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:peer, pid, :frame, %Frame{command: "notfound", payload: payload}}, state) do
    now = state.now_fun.()

    tracker =
      Enum.reduce(tx_hashes(payload), state.tracker, fn txid, tracker ->
        {tracker, _} = Tracker.step(tracker, {:notfound, txid, pid}, now)
        tracker
      end)

    {:noreply, %{state | tracker: tracker}}
  end

  # Any other post-handshake application frame is not the observer's concern.
  def handle_info({:peer, _pid, :frame, %Frame{}}, state), do: {:noreply, state}

  def handle_info(:tick, state) do
    {tracker, _} = Tracker.step(state.tracker, :tick, state.now_fun.())
    schedule_tick(state.tick_interval_ms)
    {:noreply, %{state | tracker: tracker}}
  end

  def handle_info({:request_timeout, txid}, state) do
    {tracker, _} = Tracker.step(state.tracker, {:timeout, txid}, state.now_fun.())
    {:noreply, %{state | tracker: tracker}}
  end

  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    {tracker, _} = Tracker.step(state.tracker, {:peer_down, pid}, state.now_fun.())
    {:noreply, %{state | tracker: tracker, monitors: Map.delete(state.monitors, pid)}}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  ## ── Private ──

  defp default_pipeline(tx, addresses, tokens, source),
    do: TransactionProcessor.process_tx(tx, addresses, tokens, source)

  # Perform one Tracker-emitted action. Only `:getdata` is actionable here; the
  # `:ingest` action is handled inline in the `tx` clause (it needs the parsed
  # tx). A `:getdata` writes the request to the advertising peer, arms a
  # per-request timeout, and monitors the peer for `:peer_down`.
  defp apply_action({:getdata, peer, txid}, state) do
    Peer.send_frame(peer, :getdata, Inv.serialize([{:tx, txid}]))
    Process.send_after(self(), {:request_timeout, txid}, state.request_timeout_ms)
    monitor_peer(peer, state)
  end

  defp apply_action(_action, state), do: state

  defp monitor_peer(peer, %{monitors: monitors} = state) do
    if Map.has_key?(monitors, peer) do
      state
    else
      %{state | monitors: Map.put(monitors, peer, Process.monitor(peer))}
    end
  end

  # Prefilter → matcher → pipeline. The prefilter is a cheap superset; the
  # matcher is the inclusion authority; only a real match is published.
  defp ingest(tx, state) do
    with true <- Watchlist.maybe_relevant?(state.watchlist, tx),
         {addresses, tokens} when addresses != [] or tokens != [] <- state.matcher.(tx) do
      state.pipeline.(tx, addresses, tokens, :p2p)
    else
      _ -> :ok
    end
  end

  defp tx_hashes(payload) do
    case Inv.parse(payload, max_items: @max_inv_items) do
      {:ok, items, _rest} -> for {:tx, hash} <- items, do: hash
      _ -> []
    end
  end

  defp schedule_tick(interval_ms), do: Process.send_after(self(), :tick, interval_ms)
end
