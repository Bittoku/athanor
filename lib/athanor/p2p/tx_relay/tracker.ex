defmodule Athanor.P2P.TxRelay.Tracker do
  @moduledoc """
  Pure relay-lifecycle reducer (Phase 4 T4.0, §B). Governs the outbound life of
  each of our own broadcasts with no process and no IO; the owning `TxRelay`
  feeds logical events in and performs the emitted actions.

  `step(state, event, now_ms) -> {state, actions}` where:

    * events — `{:broadcast, txid, raw_bin, targets, bar}`, `{:getdata, txid,
      peer}`, `{:inv, txid, peer}`, `{:reject, txid, peer, reason}`, `:tick`.
    * actions — `{:send_inv, peer, txid}`, `{:send_tx, peer, raw_bin}`,
      `{:propagated, txid}`, `{:rejected, txid, peer, reason}`,
      `{:unconfirmed, txid}`, `{:saturated, txid}`.

  ## State

  `pending` — `%{txid => entry}` of in-flight broadcasts, where each `entry` is:

    * `raw` — the decoded binary tx bytes, served verbatim on `getdata`.
    * `announced_to` — the set of peers we sent `inv` to (the announce targets).
    * `relayed_back` — the set of **non-target** peers that advertised the tx
      back (network propagation evidence). A peer in `announced_to` never counts.
    * `served_to` — peers we've already served the tx to (per-`(txid, peer)`
      `getdata` dedup).
    * `bar` — the propagation threshold (`held = min(2, N−1)`, supplied by the
      caller); `:propagated` fires when `bar ≥ 1` and `|relayed_back| ≥ bar`.
    * `propagated?` — makes `:propagated` fire exactly once.
    * `first_at_ms` — broadcast time, for TTL expiry.

  Bounded resources: `pending` is capped at `max_pending` (over-cap broadcasts
  yield `:saturated` and are not stored); `:tick` drops entries older than
  `ttl_ms`, emitting `:unconfirmed` for those that never propagated. Frame-body
  parsing/limits are the `TxRelay` shell's job — this reducer only sees decoded
  logical events.
  """

  defstruct pending: %{}, max_pending: 256, ttl_ms: 60_000

  @type txid :: binary()
  @type peer :: term()
  @type t :: %__MODULE__{}

  @doc """
  Builds a tracker. Options: `:max_pending` (default 256), `:ttl_ms` (default
  60_000).
  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    %__MODULE__{
      max_pending: Keyword.get(opts, :max_pending, 256),
      ttl_ms: Keyword.get(opts, :ttl_ms, 60_000)
    }
  end

  @doc "Advances the tracker by one event. See the module doc."
  @spec step(t(), tuple() | :tick, integer()) :: {t(), [tuple()]}
  def step(%__MODULE__{} = state, {:broadcast, txid, raw_bin, targets, bar}, now_ms) do
    if map_size(state.pending) >= state.max_pending do
      {state, [{:saturated, txid}]}
    else
      entry = %{
        raw: raw_bin,
        announced_to: MapSet.new(targets),
        relayed_back: MapSet.new(),
        served_to: MapSet.new(),
        bar: bar,
        propagated?: false,
        first_at_ms: now_ms
      }

      {put_entry(state, txid, entry), Enum.map(targets, &{:send_inv, &1, txid})}
    end
  end

  def step(%__MODULE__{} = state, {:getdata, txid, peer}, _now_ms) do
    case Map.get(state.pending, txid) do
      %{served_to: served, raw: raw} = entry ->
        if MapSet.member?(served, peer) do
          {state, []}
        else
          {put_entry(state, txid, %{entry | served_to: MapSet.put(served, peer)}),
           [{:send_tx, peer, raw}]}
        end

      nil ->
        {state, []}
    end
  end

  def step(%__MODULE__{} = state, {:inv, txid, peer}, _now_ms) do
    case Map.get(state.pending, txid) do
      %{} = entry ->
        # Only a non-target peer (and only before we've already declared
        # propagation) advances the count.
        if entry.propagated? or MapSet.member?(entry.announced_to, peer) do
          {state, []}
        else
          relayed = MapSet.put(entry.relayed_back, peer)

          if entry.bar >= 1 and MapSet.size(relayed) >= entry.bar do
            {put_entry(state, txid, %{entry | relayed_back: relayed, propagated?: true}),
             [{:propagated, txid}]}
          else
            {put_entry(state, txid, %{entry | relayed_back: relayed}), []}
          end
        end

      nil ->
        {state, []}
    end
  end

  # A single peer's refusal is recorded for audit but does not clear the
  # broadcast — another peer may still accept and relay it.
  def step(%__MODULE__{} = state, {:reject, txid, peer, reason}, _now_ms),
    do: {state, [{:rejected, txid, peer, reason}]}

  def step(%__MODULE__{} = state, :tick, now_ms) do
    {expired, kept} =
      Enum.split_with(state.pending, fn {_txid, e} -> e.first_at_ms + state.ttl_ms <= now_ms end)

    # Only entries that never reached propagation are surfaced as :unconfirmed;
    # an already-propagated entry has had its terminal event and is just dropped.
    actions = for {txid, e} <- expired, not e.propagated?, do: {:unconfirmed, txid}
    {%{state | pending: Map.new(kept)}, actions}
  end

  defp put_entry(state, txid, entry),
    do: %{state | pending: Map.put(state.pending, txid, entry)}
end
