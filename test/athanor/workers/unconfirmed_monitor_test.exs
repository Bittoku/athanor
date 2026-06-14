defmodule Athanor.Workers.UnconfirmedMonitorTest do
  @moduledoc """
  Covers `UnconfirmedMonitor.sweep/1`, the pure work function behind the
  periodic `:check_unconfirmed` tick. Tests inject a fetcher fn so no
  live RPC connection is needed.

  Regression: prior to the test landing alongside this file, the worker
  pattern-matched on `{:error, %{"code" => -5}}` but `RpcClient` actually
  returns `{:error, {:rpc_error, -5, msg}}`. The `:tx_deleted` PubSub
  broadcast never fired in production — silently masking dropped /
  replaced transactions for every wallet subscriber.
  """

  use Athanor.DataCase, async: false

  alias Athanor.Workers.UnconfirmedMonitor
  alias Athanor.Repo
  alias Athanor.Schema.MetaTransaction

  ## ── Helpers ──

  defp insert_unconfirmed!(opts \\ []) do
    age_seconds = Keyword.get(opts, :age_seconds, 7200)

    {:ok, meta} =
      %MetaTransaction{}
      |> MetaTransaction.changeset(%{
        txid: :crypto.strong_rand_bytes(32),
        hex: "0100000001" <> String.duplicate("00", 50),
        timestamp: System.os_time(:second) - age_seconds,
        is_confirmed: false
      })
      |> Repo.insert()

    meta
  end

  defp txid_hex(%MetaTransaction{txid: txid}), do: Base.encode16(txid, case: :lower)

  ## ── Tests ──

  describe "sweep/1 — confirmed transactions" do
    test "flips is_confirmed=true and records block_hash for confirmed txs" do
      meta = insert_unconfirmed!()
      block_hash_hex = String.duplicate("ab", 32)

      fetcher = fn _txid ->
        {:ok, %{"blockhash" => block_hash_hex, "confirmations" => 6}}
      end

      assert %{confirmed: 1, dropped: 0, still_unconfirmed: 0, errors: 0} =
               UnconfirmedMonitor.sweep(fetcher: fetcher)

      refreshed = Repo.get!(MetaTransaction, meta.id)
      assert refreshed.is_confirmed == true
      assert refreshed.block_hash == Base.decode16!(block_hash_hex, case: :mixed)
    end
  end

  describe "sweep/1 — dropped transactions (the regression)" do
    test "publishes :tx_deleted to the tx:<hex> topic on bitcoind error -5" do
      meta = insert_unconfirmed!()
      hex = txid_hex(meta)

      :ok = Phoenix.PubSub.subscribe(Athanor.PubSub, "tx:#{hex}")

      fetcher = fn _txid ->
        {:error, {:rpc_error, -5, "No such mempool or blockchain transaction"}}
      end

      assert %{confirmed: 0, dropped: 1, still_unconfirmed: 0, errors: 0} =
               UnconfirmedMonitor.sweep(fetcher: fetcher)

      assert_receive {:tx_deleted, %{txid: ^hex}}, 100

      # And the row is NOT flipped to confirmed — only the broadcast happened
      refute Repo.get!(MetaTransaction, meta.id).is_confirmed
    end

    test "respects a custom :pubsub server (broadcast does NOT leak to the default)" do
      meta = insert_unconfirmed!()
      hex = txid_hex(meta)

      start_supervised!({Phoenix.PubSub, name: :test_pubsub})

      # Subscribe to BOTH so we can prove only the custom pubsub received it.
      :ok = Phoenix.PubSub.subscribe(:test_pubsub, "tx:#{hex}")
      :ok = Phoenix.PubSub.subscribe(Athanor.PubSub, "tx:#{hex}")

      fetcher = fn _ -> {:error, {:rpc_error, -5, "missing"}} end

      assert %{dropped: 1} =
               UnconfirmedMonitor.sweep(fetcher: fetcher, pubsub: :test_pubsub)

      assert_receive {:tx_deleted, %{txid: ^hex}}, 100
      # If the default pubsub were also broadcast to, a second message would arrive.
      refute_receive {:tx_deleted, _}, 50
    end
  end

  describe "sweep/1 — still-unconfirmed and error paths" do
    test "returns a still_unconfirmed count when conf=0 and does not touch the row" do
      meta = insert_unconfirmed!()
      fetcher = fn _txid -> {:ok, %{"confirmations" => 0}} end

      assert %{still_unconfirmed: 1, confirmed: 0} =
               UnconfirmedMonitor.sweep(fetcher: fetcher)

      refute Repo.get!(MetaTransaction, meta.id).is_confirmed
    end

    test "counts non-(-5) RPC errors as :errors and leaves the row untouched" do
      meta = insert_unconfirmed!()
      fetcher = fn _txid -> {:error, {:rpc_error, -28, "Loading block index..."}} end

      assert %{errors: 1, dropped: 0, confirmed: 0} =
               UnconfirmedMonitor.sweep(fetcher: fetcher)

      refute Repo.get!(MetaTransaction, meta.id).is_confirmed
    end

    test "tolerates network-failure-style {:error, reason} tuples" do
      _meta = insert_unconfirmed!()
      fetcher = fn _txid -> {:error, {:request_failed, :econnrefused}} end

      assert %{errors: 1} = UnconfirmedMonitor.sweep(fetcher: fetcher)
    end
  end

  describe "sweep/1 — stale threshold filtering" do
    test "skips rows newer than :stale_threshold_ms" do
      _fresh = insert_unconfirmed!(age_seconds: 5)
      _stale = insert_unconfirmed!(age_seconds: 7200)

      counted = :counters.new(1, [])

      fetcher = fn _txid ->
        :counters.add(counted, 1, 1)
        {:ok, %{"confirmations" => 0}}
      end

      # 1-hour threshold (default): fresh skipped, stale checked
      assert %{still_unconfirmed: 1} = UnconfirmedMonitor.sweep(fetcher: fetcher)
      assert :counters.get(counted, 1) == 1
    end

    test "honors a relaxed :stale_threshold_ms that admits younger rows" do
      _young_a = insert_unconfirmed!(age_seconds: 10)
      _young_b = insert_unconfirmed!(age_seconds: 20)

      counted = :counters.new(1, [])

      fetcher = fn _txid ->
        :counters.add(counted, 1, 1)
        {:ok, %{"confirmations" => 0}}
      end

      assert %{still_unconfirmed: 2} =
               UnconfirmedMonitor.sweep(fetcher: fetcher, stale_threshold_ms: 1_000)

      assert :counters.get(counted, 1) == 2
    end
  end

  describe "sweep/1 — empty workload" do
    test "returns a zeroed summary when no stale rows exist" do
      assert %{confirmed: 0, dropped: 0, still_unconfirmed: 0, errors: 0} =
               UnconfirmedMonitor.sweep(fetcher: fn _ -> {:error, :unreachable} end)
    end

    test "skips confirmed rows even when stale" do
      {:ok, _} =
        %MetaTransaction{}
        |> MetaTransaction.changeset(%{
          txid: :crypto.strong_rand_bytes(32),
          hex: "0100000001" <> String.duplicate("00", 50),
          timestamp: System.os_time(:second) - 7200,
          is_confirmed: true
        })
        |> Repo.insert()

      fetcher = fn _ -> flunk("should not be called for already-confirmed rows") end

      assert %{confirmed: 0, dropped: 0, still_unconfirmed: 0, errors: 0} =
               UnconfirmedMonitor.sweep(fetcher: fetcher)
    end
  end
end
