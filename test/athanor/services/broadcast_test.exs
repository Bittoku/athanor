defmodule Athanor.Services.BroadcastTest do
  @moduledoc """
  Tests for `Athanor.Services.Broadcast.broadcast_tx/2` (Phase 4 T4.2, §C) — the
  single P2P-primary / RPC-fallback broadcast entry point, plus the
  `apply_relay_event/1` audit bridge.

  All external seams are injected: `:relay` (the synchronous TxRelay enqueue,
  `(txid_bin, raw_bin -> :ok | {:error, :saturated} | {:error, :no_peers})`),
  `:broadcaster` (`(raw_tx_hex -> {:ok, txid} | {:error, reason})`, default the
  node RPC), `:rpc_fallback?`, `:max_tx_bytes`, and `:peers_available?` (the
  live-peer routing predicate, whose production default is
  `Supervisor.enabled?/0 and PeerRegistry.pids/1 != []`). No node/socket needed.

  `async: false` — the SQL sandbox runs shared so injected stubs (other procs)
  could touch the DB, and these assert global routing behavior.
  """
  use Athanor.DataCase, async: false

  alias Athanor.Repo
  alias Athanor.Schema.Broadcast, as: Row
  alias Athanor.Services.Broadcast

  # ── Helpers ──

  # A real, parseable tx (so to_hex/txid round-trip is genuine).
  defp build_tx do
    pkh = :crypto.strong_rand_bytes(20)

    tx = %BSV.Transaction{
      version: 1,
      inputs: [
        %BSV.Transaction.Input{
          source_txid: :crypto.strong_rand_bytes(32),
          source_tx_out_index: 0,
          unlocking_script: %BSV.Script{chunks: []},
          sequence_number: 0xFFFFFFFF
        }
      ],
      outputs: [
        %BSV.Transaction.Output{satoshis: 1000, locking_script: BSV.Script.p2pkh_lock(pkh)}
      ],
      lock_time: 0
    }

    %{
      hex: BSV.Transaction.to_hex(tx),
      txid_hex: BSV.Transaction.tx_id_hex(tx),
      txid_bin: BSV.Transaction.txid_binary(tx),
      raw_bin: BSV.Transaction.to_binary(tx)
    }
  end

  defp reload(%Row{id: id}), do: Repo.get!(Row, id)

  # Records every seam invocation as a message to the test process.
  defp recording_relay(reply) do
    test = self()
    fn txid_bin, raw_bin -> send(test, {:relay, txid_bin, raw_bin}) && reply end
  end

  defp recording_broadcaster(reply) do
    test = self()
    fn hex -> send(test, {:broadcaster, hex}) && reply end
  end

  # ── Backward-compat arity ──

  test "arity-1 broadcast_tx/1 still works for the existing callers (invalid hex path)" do
    # The existing controller/channel callers invoke arity-1; it must keep
    # returning `{:ok, record}` with the display-hex txid and a status string.
    assert {:ok, record} = Broadcast.broadcast_tx("not-a-transaction")
    assert record.status == "rejected"
    assert record.error == "invalid raw transaction"
  end

  # ── Upstream validation (no relay, no inv, no RPC) ──

  test "invalid raw → row rejected with no relay and no RPC call" do
    %{} = build_tx()

    assert {:ok, record} =
             Broadcast.broadcast_tx("zzzz",
               peers_available?: true,
               relay: fn _, _ -> flunk("relay must not run for invalid input") end,
               broadcaster: fn _ -> flunk("broadcaster must not run for invalid input") end
             )

    assert record.status == "rejected"
    assert record.error == "invalid raw transaction"
  end

  test "oversized raw (> max_tx_bytes) → row rejected, not stored/served, no relay, no RPC" do
    tx = build_tx()

    assert {:ok, record} =
             Broadcast.broadcast_tx(tx.hex,
               max_tx_bytes: 10,
               peers_available?: true,
               relay: fn _, _ -> flunk("relay must not run for oversized input") end,
               broadcaster: fn _ -> flunk("broadcaster must not run for oversized input") end
             )

    assert record.status == "rejected"
    assert record.error == "transaction too large"
    assert record.txid == tx.txid_hex
  end

  # ── Cold start (P2P disabled / zero peers) — RPC only, exactly as today ──

  test "cold start (no peers) routes RPC-only and never calls the relay" do
    tx = build_tx()

    assert {:ok, record} =
             Broadcast.broadcast_tx(tx.hex,
               peers_available?: false,
               relay: recording_relay(:ok),
               broadcaster: recording_broadcaster({:ok, tx.txid_hex})
             )

    assert record.status == "accepted"
    assert_received {:broadcaster, _hex}
    refute_received {:relay, _, _}
  end

  test "cold start with an RPC error → row rejected (exactly today's behavior)" do
    tx = build_tx()

    assert {:ok, record} =
             Broadcast.broadcast_tx(tx.hex,
               peers_available?: false,
               broadcaster: fn _ -> {:error, :node_unreachable} end
             )

    assert record.status == "rejected"
    assert record.error =~ "node_unreachable"
  end

  # ── Live-peer path ──

  test "live peers, default rpc_fallback? (true): relay AND broadcaster run (relayed+accept → accepted)" do
    tx = build_tx()

    assert {:ok, record} =
             Broadcast.broadcast_tx(tx.hex,
               peers_available?: true,
               relay: recording_relay(:ok),
               broadcaster: recording_broadcaster({:ok, tx.txid_hex})
             )

    # The relay was handed the exact binary txid + wire bytes...
    assert_received {:relay, txid_bin, raw_bin}
    assert txid_bin == tx.txid_bin
    assert raw_bin == tx.raw_bin
    # ...and the belt-and-suspenders broadcaster still ran (default fallback on),
    # so per the lattice relayed(1)+accept(4) → accepted.
    assert_received {:broadcaster, _hex}
    assert record.status == "accepted"
  end

  test "live peers, rpc_fallback?: false: relay runs, broadcaster is skipped, row stays relayed" do
    tx = build_tx()

    assert {:ok, record} =
             Broadcast.broadcast_tx(tx.hex,
               peers_available?: true,
               rpc_fallback?: false,
               relay: recording_relay(:ok),
               broadcaster: fn _ ->
                 flunk("broadcaster must not run when rpc_fallback? is false")
               end
             )

    assert_received {:relay, _, _}
    assert record.status == "relayed"
  end

  test "relay saturated → falls back to broadcaster like cold start (row reflects RPC, not relayed)" do
    tx = build_tx()

    assert {:ok, record} =
             Broadcast.broadcast_tx(tx.hex,
               peers_available?: true,
               relay: fn _, _ -> {:error, :saturated} end,
               broadcaster: recording_broadcaster({:ok, tx.txid_hex})
             )

    assert_received {:broadcaster, _hex}
    assert record.status == "accepted"
  end

  test "peer-churn race: relay returns {:error, :no_peers} → RPC fallback still fires" do
    tx = build_tx()

    # broadcast_tx initially sees ≥1 peer (peers_available?: true) but the relay
    # rechecks and now reads zero — the tx must still broadcast via RPC.
    assert {:ok, record} =
             Broadcast.broadcast_tx(tx.hex,
               peers_available?: true,
               rpc_fallback?: false,
               relay: fn _, _ -> {:error, :no_peers} end,
               broadcaster: recording_broadcaster({:ok, tx.txid_hex})
             )

    assert_received {:broadcaster, _hex}
    assert record.status == "accepted"
  end

  # ── Audit identity bridge + lattice (async events) ──

  test "a binary-keyed :propagated event updates the exact row broadcast_tx inserted" do
    tx = build_tx()

    {:ok, row} =
      Broadcast.broadcast_tx(tx.hex,
        peers_available?: true,
        rpc_fallback?: false,
        relay: recording_relay(:ok)
      )

    assert row.status == "relayed"
    # The row's txid stays display-order hex (backward-compatible column)...
    assert row.txid == tx.txid_hex
    assert row.txid =~ ~r/^[0-9a-f]{64}$/

    # ...and a *binary*-keyed event resolves to that exact row via display_hex.
    assert :ok = Broadcast.apply_relay_event({:propagated, tx.txid_bin})
    assert reload(row).status == "propagated"
  end

  test "reject-then-TTL: a rejected row is NOT overwritten by a later :unconfirmed" do
    tx = build_tx()

    {:ok, row} =
      Broadcast.broadcast_tx(tx.hex,
        peers_available?: false,
        broadcaster: fn _ -> {:error, :bad} end
      )

    assert row.status == "rejected"
    assert :ok = Broadcast.apply_relay_event({:unconfirmed, tx.txid_bin})
    assert reload(row).status == "rejected"
  end

  test "TTL-then-reject: an unconfirmed row IS overwritten by a later peer :rejected" do
    tx = build_tx()

    {:ok, row} =
      Broadcast.broadcast_tx(tx.hex,
        peers_available?: true,
        rpc_fallback?: false,
        relay: recording_relay(:ok)
      )

    assert :ok = Broadcast.apply_relay_event({:unconfirmed, tx.txid_bin})
    assert reload(row).status == "unconfirmed"
    assert :ok = Broadcast.apply_relay_event({:rejected, tx.txid_bin, :peer, "16: bad"})
    assert reload(row).status == "rejected"
  end

  test "apply_relay_event for an unknown txid is a no-op" do
    assert :ok = Broadcast.apply_relay_event({:propagated, :binary.copy(<<0x7E>>, 32)})
  end

  # ── Phase 5 T5.4: broadcast is router-driven ──

  test "routing :broadcast away from :p2p forces RPC-only even with live peers" do
    Application.put_env(:athanor, Athanor.P2P.SourceRouter, routes: %{broadcast: {:rpc, []}})
    on_exit(fn -> Application.delete_env(:athanor, Athanor.P2P.SourceRouter) end)

    tx = build_tx()

    assert {:ok, record} =
             Broadcast.broadcast_tx(tx.hex,
               peers_available?: true,
               relay: fn _, _ ->
                 flunk("relay must not run when router routes broadcast to :rpc")
               end,
               broadcaster: recording_broadcaster({:ok, tx.txid_hex})
             )

    assert_received {:broadcaster, _hex}
    assert record.status == "accepted"
  end

  test "registry unavailable while P2P enabled: broadcast fails closed to the RPC path" do
    # P2P enabled, but no PeerRegistry process → PeerRegistry.pids/0 exits. The
    # peer gate must fail closed to RPC rather than crash before the fallback.
    Application.put_env(:athanor, Athanor.P2P, enabled: true)
    on_exit(fn -> Application.delete_env(:athanor, Athanor.P2P) end)

    tx = build_tx()

    assert {:ok, record} =
             Broadcast.broadcast_tx(tx.hex,
               relay: fn _, _ -> flunk("relay must not run when the registry is unavailable") end,
               broadcaster: recording_broadcaster({:ok, tx.txid_hex})
             )

    assert_received {:broadcaster, _hex}
    assert record.status == "accepted"
  end
end
