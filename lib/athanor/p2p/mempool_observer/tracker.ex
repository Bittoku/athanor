defmodule Athanor.P2P.MempoolObserver.Tracker do
  @moduledoc """
  Pure mempool request-lifecycle reducer (Phase 3 T3.2, §B). Governs the
  `inv → getdata → tx` exchange with no process and no IO; the owning
  `MempoolObserver` only feeds events in and performs the emitted actions.

  ## Model

  `step(state, event, now_ms) -> {state, actions}` where:

    * events — `{:inv, txid, peer}`, `{:tx, txid, payload, peer}`,
      `{:notfound, txid, peer}`, `{:timeout, txid}`, `{:peer_down, peer}`, `:tick`.
    * actions — `[{:getdata, peer, txid}]` | `[{:ingest, payload}]` | `[]`.

  ## State (the in-flight / completed split)

    * `outstanding` — `%{txid => {peer, requested_at_ms}}`, the **in-flight**
      requests. While a txid is outstanding, concurrent `inv`s are ignored
      (flood dedupe).
    * `seen` — `%{txid => until_ms}`, txids **already successfully ingested**,
      with TTL deadlines. **Set only on a successful `tx`** — never on `inv` and
      never on a failure. Dedupes *completed* work.
    * token-bucket (`tokens`/`max_tokens`/`refill`) — rate-limits `getdata`.

  A txid is dedup-suppressed iff it is `outstanding` **or** `seen`. A failure
  (`notfound`/`timeout`/`peer_down`) clears `outstanding` and leaves the txid
  neither outstanding nor seen, so a *different* peer's later `inv` re-requests
  it (recovery), while a flood of simultaneous `inv`s is still collapsed to one.

  ## Peer-matched delivery

  An outstanding request is bound to the peer it was sent to. Only that peer can
  satisfy or cancel it: a `tx` or `notfound` carrying a *different* peer is
  ignored and leaves the request outstanding (so the asked peer — or a later
  recovery `inv` — can still deliver). This keeps request accounting
  peer-unambiguous: a stranger's `notfound` cannot cancel peer A's in-flight
  request, and an unsolicited `tx` cannot mark another peer's request complete.
  """

  defstruct outstanding: %{},
            seen: %{},
            tokens: 200,
            max_tokens: 200,
            refill: 200,
            seen_ttl_ms: 600_000,
            request_timeout_ms: 30_000

  @type txid :: binary()
  @type peer :: term()
  @type action :: {:getdata, peer(), txid()} | {:ingest, binary()}
  @type t :: %__MODULE__{}

  @doc """
  Builds a tracker. Options: `:max_tokens` (also the initial token count and
  per-tick refill cap), `:refill`, `:seen_ttl_ms`, `:request_timeout_ms`.
  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    max = Keyword.get(opts, :max_tokens, 200)

    %__MODULE__{
      tokens: max,
      max_tokens: max,
      refill: Keyword.get(opts, :refill, max),
      seen_ttl_ms: Keyword.get(opts, :seen_ttl_ms, 600_000),
      request_timeout_ms: Keyword.get(opts, :request_timeout_ms, 30_000)
    }
  end

  @doc "Advances the tracker by one event. See the module doc."
  @spec step(t(), tuple() | :tick, integer()) :: {t(), [action()]}
  def step(%__MODULE__{} = state, {:inv, txid, peer}, now_ms) do
    cond do
      Map.has_key?(state.outstanding, txid) -> {state, []}
      seen?(state, txid, now_ms) -> {state, []}
      state.tokens <= 0 -> {state, []}
      true -> {request(state, txid, peer, now_ms), [{:getdata, peer, txid}]}
    end
  end

  def step(%__MODULE__{} = state, {:tx, txid, payload, peer}, now_ms) do
    # Only the peer we asked can satisfy the request. An unsolicited / stale
    # `tx` from a different peer is ignored and leaves the request outstanding,
    # so the asked peer (or a later recovery) can still deliver.
    if requested_from?(state, txid, peer) do
      state = %{
        state
        | outstanding: Map.delete(state.outstanding, txid),
          seen: Map.put(state.seen, txid, now_ms + state.seen_ttl_ms)
      }

      {state, [{:ingest, payload}]}
    else
      {state, []}
    end
  end

  # A `notfound` only cancels the request if it comes from the peer we asked;
  # a stranger peer's `notfound` must not cancel peer A's in-flight request.
  def step(%__MODULE__{} = state, {:notfound, txid, peer}, _now_ms) do
    if requested_from?(state, txid, peer) do
      {drop_outstanding(state, txid), []}
    else
      {state, []}
    end
  end

  def step(%__MODULE__{} = state, {:timeout, txid}, _now_ms),
    do: {drop_outstanding(state, txid), []}

  def step(%__MODULE__{} = state, {:peer_down, peer}, _now_ms) do
    outstanding = Map.reject(state.outstanding, fn {_txid, {p, _at}} -> p == peer end)
    {%{state | outstanding: outstanding}, []}
  end

  def step(%__MODULE__{} = state, :tick, now_ms) do
    state = %{
      state
      | tokens: min(state.max_tokens, state.tokens + state.refill),
        seen: Map.reject(state.seen, fn {_txid, until} -> until <= now_ms end),
        outstanding: expire_outstanding(state.outstanding, now_ms, state.request_timeout_ms)
    }

    {state, []}
  end

  defp request(state, txid, peer, now_ms) do
    %{
      state
      | tokens: state.tokens - 1,
        outstanding: Map.put(state.outstanding, txid, {peer, now_ms})
    }
  end

  defp drop_outstanding(state, txid),
    do: %{state | outstanding: Map.delete(state.outstanding, txid)}

  # True iff `txid` is outstanding AND was requested from exactly `peer`.
  defp requested_from?(state, txid, peer) do
    case Map.get(state.outstanding, txid) do
      {^peer, _at} -> true
      _ -> false
    end
  end

  defp seen?(state, txid, now_ms) do
    case Map.get(state.seen, txid) do
      nil -> false
      until -> until > now_ms
    end
  end

  defp expire_outstanding(outstanding, now_ms, timeout_ms) do
    Map.reject(outstanding, fn {_txid, {_peer, at}} -> at + timeout_ms <= now_ms end)
  end
end
