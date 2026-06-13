defmodule Athanor.P2P.TxFetcher do
  @moduledoc """
  Phase 5 T5.2 (§B) — the thin GenServer that pulls a **specific** tx by id from
  the peer set via `getdata`. It is a `frame_sink` member (§A fan-out): the pool
  forwards each post-handshake frame as `{:peer, pid, :frame, %Frame{}}`, and this
  shell acts only on `tx`/`notfound` for txids it is actively fetching.

  All *decisions* live in the pure `TxFetcher.Tracker`; this shell does IO and
  holds the synchronous caller's reply until the Tracker resolves.

  ## Pull-fetch entry point

    * `fetch(server, txid, opts)` — a **synchronous** `GenServer.call`. It rechecks
      `PeerRegistry.pids/1` and:
        * returns `:miss` immediately if there are **zero** peers (the cold-start
          gate — no `getdata`, no waiter), so a `:p2p`-primary caller falls
          straight through to its REST/RPC fallback;
        * otherwise picks up to `:fanout` peers via the injected `:selector`,
          steps `{:request, txid, peers}` (sending `getdata(MSG_TX, txid)` to each),
          and **holds the reply** until the Tracker resolves `{:ok, raw_bin}` (a
          matching `tx` from an asked peer) or `:miss` (all asked peers `notfound`,
          or the `timeout_ms` deadline passes). Mempool-only: a node only serves
          `getdata` for mempool txs, so confirmed txs miss and fall back.

  ## Inbound frames
    * `tx` → re-hash the payload to its txid (forgery guard: a payload whose hash
      isn't a pending request never resolves anything), then fold `{:tx, txid, raw,
      peer}`.
    * `notfound` → fold `{:notfound, txid, peer}` for each advertised tx hash.
    * `:tick` → expire pending requests past `timeout_ms`.

  Multiple callers awaiting the same txid are all replied to on resolve. Injected
  collaborators: `:registry`, `:selector` (`(pids, fanout) -> [peer]`, default
  random shuffle/take), `:fanout` (default 3), `:now_fun`, `:tick_interval_ms`,
  `:timeout_ms`, `:call_timeout` (the outer `GenServer.call` budget).
  """

  use GenServer

  alias Athanor.P2P.{Frame, Peer, PeerRegistry}
  alias Athanor.P2P.Messages.Inv
  alias Athanor.P2P.TxFetcher.Tracker

  @max_inv_items 50_000

  ## ── Client API ──

  @doc "Starts the fetcher. See the module doc for options."
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    {name, opts} = Keyword.pop(opts, :name)
    GenServer.start_link(__MODULE__, opts, if(name, do: [name: name], else: []))
  end

  @doc """
  Synchronously pull-fetches `txid` from the live peer set. Returns
  `{:ok, raw_bin}` on a hash-verified mempool hit, or `:miss` (zero peers, all
  `notfound`, or timeout). `opts[:call_timeout]` bounds the outer call wait.

  The server is **explicit** (no default) so the arity is unambiguous: `fetch/2`
  is always `(server, txid)` and `fetch/3` is `(server, txid, opts)` — there is no
  arity where `(txid, opts)` could be misparsed as `(server, txid)`. Call the
  supervised fetcher as `fetch(#{inspect(__MODULE__)}, txid, opts)`.
  """
  @spec fetch(GenServer.server(), Tracker.txid(), keyword()) :: {:ok, binary()} | :miss
  def fetch(server, txid, opts \\ []) do
    GenServer.call(server, {:fetch, txid}, Keyword.get(opts, :call_timeout, 10_000))
  end

  ## ── Server callbacks ──

  @impl true
  def init(opts) do
    tracker =
      case Keyword.get(opts, :tracker) do
        %Tracker{} = t -> t
        o when is_list(o) -> Tracker.new(o)
        nil -> Tracker.new(Keyword.take(opts, [:timeout_ms]))
      end

    state = %{
      tracker: tracker,
      registry: Keyword.get(opts, :registry, PeerRegistry),
      selector: Keyword.get(opts, :selector, &default_selector/2),
      fanout: Keyword.get(opts, :fanout, 3),
      now_fun: Keyword.get(opts, :now_fun, fn -> System.monotonic_time(:millisecond) end),
      tick_interval_ms: Keyword.get(opts, :tick_interval_ms, 500),
      # txid => [GenServer.from] awaiting that fetch.
      waiters: %{}
    }

    schedule_tick(state.tick_interval_ms)
    {:ok, state}
  end

  @impl true
  def handle_call({:fetch, txid}, from, state) do
    case safe_pids(state.registry) do
      [] ->
        # Cold-start gate: no peers → instant miss, before any getdata. A registry
        # that is absent/restarting makes `pids/1` exit; `safe_pids/1` normalizes
        # that to `[]` so the fetch fails closed (`:miss`) here instead of crashing
        # the fetcher (which would drop any in-flight waiters for other txids).
        {:reply, :miss, state}

      pids ->
        peers = state.selector.(pids, state.fanout)
        {tracker, actions} = Tracker.step(state.tracker, {:request, txid, peers}, now(state))
        state = %{state | tracker: tracker, waiters: add_waiter(state.waiters, txid, from)}
        send_getdatas(actions)
        {:noreply, state}
    end
  end

  @impl true
  def handle_info({:peer, pid, :frame, %Frame{command: "tx", payload: payload}}, state) do
    case BSV.Transaction.from_binary(payload) do
      {:ok, tx, _rest} ->
        txid = BSV.Transaction.txid_binary(tx)
        {tracker, actions} = Tracker.step(state.tracker, {:tx, txid, payload, pid}, now(state))
        state = %{state | tracker: tracker}
        {:noreply, resolve_all(actions, state)}

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:peer, pid, :frame, %Frame{command: "notfound", payload: payload}}, state) do
    now = now(state)

    {tracker, actions} =
      payload
      |> tx_hashes()
      |> Enum.reduce({state.tracker, []}, fn txid, {tracker, acc} ->
        {tracker, new} = Tracker.step(tracker, {:notfound, txid, pid}, now)
        {tracker, acc ++ new}
      end)

    {:noreply, resolve_all(actions, %{state | tracker: tracker})}
  end

  def handle_info({:peer, _pid, :frame, %Frame{}}, state), do: {:noreply, state}

  def handle_info(:tick, state) do
    {tracker, actions} = Tracker.step(state.tracker, :tick, now(state))
    state = resolve_all(actions, %{state | tracker: tracker})
    schedule_tick(state.tick_interval_ms)
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  ## ── Private ──

  defp send_getdatas(actions) do
    Enum.each(actions, fn {:send_getdata, peer, txid} ->
      Peer.send_frame(peer, :getdata, Inv.serialize([{:tx, txid}]))
    end)
  end

  # Reply every waiter for each resolved txid and clear it.
  defp resolve_all(actions, state) do
    Enum.reduce(actions, state, fn {:resolve, txid, result}, st ->
      for from <- Map.get(st.waiters, txid, []), do: GenServer.reply(from, result)
      %{st | waiters: Map.delete(st.waiters, txid)}
    end)
  end

  defp add_waiter(waiters, txid, from), do: Map.update(waiters, txid, [from], &[from | &1])

  defp tx_hashes(payload) do
    case Inv.parse(payload, max_items: @max_inv_items) do
      {:ok, items, _rest} -> for {:tx, hash} <- items, do: hash
      _ -> []
    end
  end

  # Default production selector: up to `fanout` randomly-shuffled live peers (so a
  # pull-fetch doesn't always hammer the same peer / leak a stable pattern).
  defp default_selector(pids, fanout), do: pids |> Enum.shuffle() |> Enum.take(fanout)

  # `PeerRegistry.pids/1` is a `GenServer.call` — a registry that is absent or
  # restarting makes it exit. Treat that as "no peers" so a fetch fails closed to
  # `:miss` rather than crashing the fetcher (and dropping other waiters).
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
