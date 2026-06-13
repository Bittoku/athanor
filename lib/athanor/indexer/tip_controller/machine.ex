defmodule Athanor.Indexer.TipController.Machine do
  @moduledoc """
  Phase 7 F7.2 (T7.0) — the **pure** authority state machine for `TipController`.

  The thin node has exactly one index-tip mutation authority: the RPC-confirmed
  reconcile cycle. This reducer decides *when* a cycle runs and tracks the
  authority phase; it performs no IO. The owning GenServer (`TipController`)
  executes the `:reconcile` action and feeds the outcome back as a
  `{:cycle_result, …}` event.

  `step(state, event) -> {state, [action]}`

    * **phases** — `:bootstrapping` (no contiguous prefix yet) → `:syncing` (below
      the node tip / recovering) → `:synced` (index hash-equals the node tip).
    * **events** —
      * `:tick` / `{:hint, source}` — a poll tick or an advisory hint from any
        producer (`:p2p | :zmq | :junglebus | …`); both *trigger a reconcile*,
        coalesced (see below).
      * `{:cycle_result, result}` — the outcome of a reconcile cycle, one of
        `:bootstrapped | :synced | :progressed | :deferred`.
    * **actions** — `:reconcile` (run a cycle) | `:noop`.

  ## Coalescing
  At most one cycle is "in flight". A tick/hint while a cycle runs does not start a
  second cycle — it sets `pending`, so exactly one follow-up runs when the current
  cycle returns. A `:progressed` result (more chain to apply) also schedules an
  immediate follow-up; `:synced`/`:deferred` settle until the next trigger.
  """

  @type phase :: :bootstrapping | :syncing | :synced
  @type result :: :bootstrapped | :synced | :progressed | :deferred
  @type event :: :tick | {:hint, atom()} | {:cycle_result, result()}
  @type action :: :reconcile | :noop

  @enforce_keys [:phase, :in_flight, :pending]
  defstruct phase: :bootstrapping, in_flight: false, pending: false

  @type t :: %__MODULE__{phase: phase(), in_flight: boolean(), pending: boolean()}

  @doc "A fresh machine: `:bootstrapping`, idle."
  @spec new() :: t()
  def new, do: %__MODULE__{phase: :bootstrapping, in_flight: false, pending: false}

  @doc "Advances the machine by one event. See the module doc."
  @spec step(t(), event()) :: {t(), [action()]}
  def step(%__MODULE__{} = m, :tick), do: trigger(m)
  def step(%__MODULE__{} = m, {:hint, _source}), do: trigger(m)

  def step(%__MODULE__{} = m, {:cycle_result, result}) do
    phase = fold_phase(m.phase, result)

    # Run a follow-up cycle if the chain advanced (more to do) or a trigger arrived
    # while this cycle was running.
    if result == :progressed or m.pending do
      {%{m | phase: phase, in_flight: true, pending: false}, [:reconcile]}
    else
      {%{m | phase: phase, in_flight: false, pending: false}, [:noop]}
    end
  end

  # A reconcile result folds into the phase:
  #   :bootstrapped — the boundary block is recorded → leave bootstrapping
  #   :synced       — index == node tip
  #   :progressed   — applied a branch, more may remain → keep syncing
  #   :deferred     — no progress this cycle (RPC hiccup / unknown hash) → phase unchanged
  defp fold_phase(_phase, :bootstrapped), do: :syncing
  defp fold_phase(_phase, :synced), do: :synced
  defp fold_phase(_phase, :progressed), do: :syncing
  defp fold_phase(phase, :deferred), do: phase

  # A trigger (tick or hint): start a cycle if idle, else coalesce into `pending`.
  defp trigger(%__MODULE__{in_flight: true} = m), do: {%{m | pending: true}, [:noop]}
  defp trigger(%__MODULE__{} = m), do: {%{m | in_flight: true}, [:reconcile]}
end
