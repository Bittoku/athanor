defmodule Athanor.P2P.TxFetcher.Tracker do
  @moduledoc """
  Pure pull-fetch reducer (Phase 5 T5.1, §B). Governs one `getdata`-by-txid
  request with no process and no IO; the owning `TxFetcher` feeds logical events
  in and performs the emitted actions.

  `step(state, event, now_ms) -> {state, actions}` where:

    * events — `{:request, txid, peers}`, `{:tx, txid, raw_bin, peer}`,
      `{:notfound, txid, peer}`, `:tick`.
    * actions — `{:send_getdata, peer, txid}`, `{:resolve, txid, {:ok, raw_bin}}`,
      `{:resolve, txid, :miss}`.

  ## Lifecycle (mempool-only pull-fetch)

  A node answers `getdata(MSG_TX)` only for txs in its mempool, so this fetch is a
  best-effort fast path: a hit avoids a REST round-trip for an unconfirmed parent;
  a miss falls through to the caller's REST/RPC cascade.

    * `{:request, txid, peers}` — ask up to N distinct peers; record `asked` and
      `first_at_ms`. A duplicate request for an already-pending txid is ignored.
    * `{:tx, txid, raw_bin, peer}` — the shell has already re-hashed the payload to
      `txid` (so the payload provably matches), so the only remaining guard is
      `peer ∈ asked`. On a match → resolve `{:ok, raw_bin}`. A tx from an unasked
      peer, or for a txid we aren't fetching, is ignored.
    * `{:notfound, txid, peer}` — drop `peer` from `asked`; when `asked` empties →
      resolve `:miss`.
    * `:tick` — any pending request older than `timeout_ms` resolves `:miss`.

  Resolve fires **exactly once**: the pending entry is dropped on resolve, so any
  later frame for that txid finds no entry and is ignored.

  ## State
  `pending` — `%{txid => %{asked: MapSet(peer), first_at_ms: t}}`; `timeout_ms`
  bounds how long a request waits before it is declared a miss.
  """

  defstruct pending: %{}, timeout_ms: 3_000

  @type txid :: binary()
  @type peer :: term()
  @type t :: %__MODULE__{}

  @doc "Builds a tracker. Options: `:timeout_ms` (default 3_000)."
  @spec new(keyword()) :: t()
  def new(opts \\ []), do: %__MODULE__{timeout_ms: Keyword.get(opts, :timeout_ms, 3_000)}

  @doc "Advances the tracker by one event. See the module doc."
  @spec step(t(), tuple() | :tick, integer()) :: {t(), [tuple()]}
  def step(%__MODULE__{} = state, {:request, txid, peers}, now_ms) do
    if Map.has_key?(state.pending, txid) do
      {state, []}
    else
      entry = %{asked: MapSet.new(peers), first_at_ms: now_ms}
      {put_entry(state, txid, entry), Enum.map(peers, &{:send_getdata, &1, txid})}
    end
  end

  def step(%__MODULE__{} = state, {:tx, txid, raw_bin, peer}, _now_ms) do
    case Map.get(state.pending, txid) do
      %{asked: asked} ->
        if MapSet.member?(asked, peer),
          do: {drop(state, txid), [{:resolve, txid, {:ok, raw_bin}}]},
          else: {state, []}

      nil ->
        {state, []}
    end
  end

  def step(%__MODULE__{} = state, {:notfound, txid, peer}, _now_ms) do
    case Map.get(state.pending, txid) do
      %{asked: asked} = entry ->
        asked = MapSet.delete(asked, peer)

        if MapSet.size(asked) == 0,
          do: {drop(state, txid), [{:resolve, txid, :miss}]},
          else: {put_entry(state, txid, %{entry | asked: asked}), []}

      nil ->
        {state, []}
    end
  end

  def step(%__MODULE__{} = state, :tick, now_ms) do
    {expired, kept} =
      Enum.split_with(state.pending, fn {_txid, e} ->
        e.first_at_ms + state.timeout_ms <= now_ms
      end)

    actions = for {txid, _e} <- expired, do: {:resolve, txid, :miss}
    {%{state | pending: Map.new(kept)}, actions}
  end

  defp put_entry(state, txid, entry), do: %{state | pending: Map.put(state.pending, txid, entry)}
  defp drop(state, txid), do: %{state | pending: Map.delete(state.pending, txid)}
end
