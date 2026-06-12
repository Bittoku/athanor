defmodule Athanor.Services.Broadcast do
  @moduledoc """
  Single transaction-broadcast entry point (Phase 4 §C): **P2P-primary, RPC/REST
  fallback**. Records every attempt in the `broadcasts` table for audit.

  `broadcast_tx/2` decodes the raw hex **once**, validates it upstream, then
  routes:

    * **cold start** (P2P disabled or zero live peers) → RPC/REST only, exactly
      as before P2P existed;
    * **live peers** → hand the tx to the relay (a synchronous enqueue). On `:ok`
      the row is `relayed`; the RPC/REST broadcaster then also runs as a
      belt-and-suspenders fallback **iff `:rpc_fallback?`** (the default), which
      (per the lattice) lifts `relayed` to `accepted`. On `{:error, :saturated |
      :no_peers}` (capacity, or peers raced to zero) it degrades to the RPC path
      exactly like cold start — the tx still broadcasts, never silently dropped.

  Validation is upstream and synchronous (so the relay never sees bad input):
  invalid/unparseable hex → row `rejected` (`error: "invalid raw transaction"`);
  `byte_size(raw_bin) > max_tx_bytes` → row `rejected` (`error: "transaction too
  large"`) — both with **no** relay, **no** `inv`, **no** RPC.

  The audit row's `txid` stays **display-order hex** (`tx_id_hex/1`, unchanged
  column / return shape). The relay/Tracker key on the **binary** wire-order
  txid; `apply_relay_event/1` is the async audit sink that converts a binary
  event txid back to display hex and advances the matching row through the
  monotonic `Broadcast.advance_status/2` lattice (never downgrading).

  ## Injected seams (`opts`)
    * `:relay` — `(txid_bin, raw_bin -> :ok | {:error, :saturated} | {:error,
      :no_peers})` (default: a `TxRelay.broadcast/3` call).
    * `:broadcaster` — `(raw_tx_hex -> {:ok, txid} | {:error, reason})` (default:
      `RpcClient.send_raw_transaction/1`).
    * `:rpc_fallback?` — gate the *post-relay* belt-and-suspenders broadcaster
      only (default `true`; config `rpc_fallback`). The cold-start path always
      calls the broadcaster regardless.
    * `:max_tx_bytes` — single-tx cap (default 1_000_000; config
      `relay: [max_tx_bytes: …]`).
    * `:peers_available?` — the live-peer routing predicate (default
      `Supervisor.enabled?/0 and PeerRegistry.pids/1 != []`).
  """

  alias Athanor.Blockchain.RpcClient
  alias Athanor.P2P.{PeerRegistry, SourceRouter, Supervisor, TxRelay}
  alias Athanor.Repo
  alias Athanor.Schema.Broadcast

  require Logger

  @default_max_tx_bytes 1_000_000

  @doc """
  Broadcasts a raw transaction hex (P2P-primary, RPC fallback) and records the
  attempt.

  ## Parameters
    - `raw_tx_hex` — raw transaction in hex encoding.
    - `opts` — injected seams (see the module doc); all default to production
      behavior, so the existing arity-1 callers are unaffected.

  ## Returns
    `{:ok, broadcast}` — the `Broadcast` row reflecting the final status
    (`accepted`/`rejected`/`relayed`; later lifted to `propagated`/`unconfirmed`
    out-of-band via `apply_relay_event/1`).
  """
  @spec broadcast_tx(String.t(), keyword()) :: {:ok, Broadcast.t()}
  def broadcast_tx(raw_tx_hex, opts \\ []) do
    case BSV.Transaction.from_hex(raw_tx_hex) do
      {:ok, tx} ->
        raw_bin = BSV.Transaction.to_binary(tx)
        txid_hex = BSV.Transaction.tx_id_hex(tx)
        txid_bin = BSV.Transaction.txid_binary(tx)

        if byte_size(raw_bin) > max_tx_bytes(opts) do
          insert_rejected(raw_tx_hex, txid_hex, "transaction too large")
        else
          route(raw_tx_hex, txid_hex, txid_bin, raw_bin, opts)
        end

      {:error, _} ->
        insert_rejected(raw_tx_hex, "unknown", "invalid raw transaction")
    end
  end

  @doc """
  Async audit sink for relay lifecycle events (the `TxRelay` `:audit` callback).
  Each event carries the **binary** wire-order txid; this converts it to the
  row's display-hex key and advances the matching `broadcasts` row through the
  monotonic lattice. A no-op if no such row exists.

  ## Parameters
    - `event` — `{:propagated, txid_bin}` | `{:unconfirmed, txid_bin}` |
      `{:rejected, txid_bin, peer, reason}`.

  ## Returns
    `:ok`.
  """
  @spec apply_relay_event(tuple()) :: :ok
  def apply_relay_event({:propagated, txid_bin}), do: advance_by_txid(txid_bin, "propagated")
  def apply_relay_event({:unconfirmed, txid_bin}), do: advance_by_txid(txid_bin, "unconfirmed")

  def apply_relay_event({:rejected, txid_bin, _peer, reason}),
    do: advance_by_txid(txid_bin, "rejected", to_string(reason))

  @doc """
  Returns recent broadcast attempts.
  """
  def list_recent(limit \\ 20) do
    import Ecto.Query

    Broadcast
    |> order_by([b], desc: b.inserted_at)
    |> limit(^limit)
    |> Repo.all()
  end

  ## ── Routing ──

  defp route(raw_tx_hex, txid_hex, txid_bin, raw_bin, opts) do
    {:ok, broadcast} =
      %Broadcast{}
      |> Broadcast.changeset(%{txid: txid_hex, hex: raw_tx_hex, status: "pending"})
      |> Repo.insert()

    if p2p_broadcast?(opts) do
      relay_route(broadcast, raw_tx_hex, txid_bin, raw_bin, opts)
    else
      # Cold start (P2P disabled / no live peers), or the router has routed
      # `:broadcast` away from `:p2p` — exactly today's RPC-only path.
      rpc_broadcast(broadcast, raw_tx_hex, opts)
    end
  end

  defp relay_route(broadcast, raw_tx_hex, txid_bin, raw_bin, opts) do
    case relay_fun(opts).(txid_bin, raw_bin) do
      :ok ->
        broadcast = advance(broadcast, "relayed")

        if rpc_fallback?(opts),
          do: rpc_broadcast(broadcast, raw_tx_hex, opts),
          else: {:ok, broadcast}

      {:error, reason} when reason in [:saturated, :no_peers] ->
        # P2P can't carry it (capacity, or peers raced to zero) — degrade to the
        # RPC path exactly like cold start so the tx still broadcasts.
        Logger.debug("TxRelay enqueue #{inspect(reason)}; falling back to RPC broadcast")
        rpc_broadcast(broadcast, raw_tx_hex, opts)
    end
  end

  defp rpc_broadcast(broadcast, raw_tx_hex, opts) do
    case broadcaster(opts).(raw_tx_hex) do
      {:ok, _returned_txid} -> {:ok, advance(broadcast, "accepted")}
      {:error, reason} -> {:ok, advance(broadcast, "rejected", inspect(reason))}
    end
  end

  ## ── Audit-row helpers ──

  defp insert_rejected(raw_tx_hex, txid_hex, error) do
    %Broadcast{}
    |> Broadcast.changeset(%{txid: txid_hex, hex: raw_tx_hex, status: "rejected", error: error})
    |> Repo.insert()
  end

  # Advance a row's status up the lattice, attaching `error` only when the row
  # actually moves to that (error-bearing) status — a downgraded event must not
  # smear its error onto a higher-ranked row.
  defp advance(broadcast, incoming, error \\ nil) do
    new_status = Broadcast.advance_status(broadcast.status, incoming)

    attrs =
      if not is_nil(error) and new_status == incoming,
        do: %{status: new_status, error: error},
        else: %{status: new_status}

    {:ok, updated} = broadcast |> Broadcast.changeset(attrs) |> Repo.update()
    updated
  end

  defp advance_by_txid(txid_bin, status, error \\ nil) do
    case Repo.get_by(Broadcast, txid: display_hex(txid_bin)) do
      nil -> :ok
      broadcast -> _ = advance(broadcast, status, error)
    end

    :ok
  end

  # Wire/internal order → display-order (block-explorer) lower-hex, the row key.
  defp display_hex(<<_::binary-size(32)>> = txid_bin) do
    txid_bin
    |> :binary.bin_to_list()
    |> Enum.reverse()
    |> :binary.list_to_bin()
    |> Base.encode16(case: :lower)
  end

  ## ── Injected-seam resolution ──

  defp relay_fun(opts), do: Keyword.get(opts, :relay, &default_relay/2)
  defp default_relay(txid_bin, raw_bin), do: TxRelay.broadcast(txid_bin, raw_bin)

  defp broadcaster(opts),
    do: Keyword.get(opts, :broadcaster, &RpcClient.send_raw_transaction/1)

  defp rpc_fallback?(opts),
    do:
      Keyword.get_lazy(opts, :rpc_fallback?, fn -> Keyword.get(p2p_env(), :rpc_fallback, true) end)

  defp max_tx_bytes(opts) do
    Keyword.get_lazy(opts, :max_tx_bytes, fn ->
      p2p_env() |> Keyword.get(:relay, []) |> Keyword.get(:max_tx_bytes, @default_max_tx_bytes)
    end)
  end

  defp peers_available?(opts) do
    Keyword.get_lazy(opts, :peers_available?, fn ->
      Supervisor.enabled?() and registry_has_peers?()
    end)
  end

  # `PeerRegistry.pids/0` is a `GenServer.call` that **exits** if the registry is
  # temporarily absent (supervisor restart / cold start). Fail **closed** to the
  # RPC path rather than crash before `rpc_broadcast/3` is reached.
  defp registry_has_peers? do
    PeerRegistry.pids() != []
  rescue
    _ -> false
  catch
    :exit, _ -> false
  end

  # The P2P relay path is taken only when the capability router routes
  # `:broadcast` to `:p2p` (the default) **and** live peers exist. Routing
  # `:broadcast` to a non-P2P provider via config forces the RPC-only path even
  # with peers — so P2P-vs-RPC is one routing decision (§C), config not hardcode.
  defp p2p_broadcast?(opts) do
    match?({:p2p, _fallbacks}, SourceRouter.resolve(:broadcast)) and peers_available?(opts)
  end

  defp p2p_env, do: Application.get_env(:athanor, Athanor.P2P, [])
end
