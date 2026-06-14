defmodule Athanor.Workers.MissingTxSyncerTest do
  @moduledoc """
  Covers `MissingTxSyncer.sync_once/1`, the pure work function behind the
  periodic `:sync_missing` tick. Tests inject fetchers and a processor so
  no live WhatsOnChain or TransactionFilter dependency is required.
  """

  use Athanor.DataCase, async: false

  alias Athanor.Workers.MissingTxSyncer
  alias Athanor.Repo
  alias Athanor.Schema.{WatchingAddress, MetaTransaction}

  ## ── Helpers ──

  defp insert_address!(addr) do
    {:ok, _} =
      %WatchingAddress{}
      |> WatchingAddress.changeset(%{address: addr, name: "test"})
      |> Repo.insert()

    addr
  end

  defp insert_known_tx!(txid_hex) do
    {:ok, _} =
      %MetaTransaction{}
      |> MetaTransaction.changeset(%{
        txid: Base.decode16!(txid_hex, case: :mixed),
        hex: "0100000001" <> String.duplicate("00", 50),
        timestamp: System.os_time(:second),
        is_confirmed: true
      })
      |> Repo.insert()

    txid_hex
  end

  defp rand_txid_hex, do: :crypto.strong_rand_bytes(32) |> Base.encode16(case: :lower)

  defp recording_processor do
    {:ok, agent} = Agent.start_link(fn -> [] end)

    {fn binary -> Agent.update(agent, &[binary | &1]) end,
     fn -> agent |> Agent.get(& &1) |> Enum.reverse() end}
  end

  ## ── Tests ──

  describe "sync_once/1 — empty workload" do
    test "returns a zeroed summary when no addresses are watched" do
      assert %{addresses: 0, backfilled: 0, already_known: 0, errors: 0} =
               MissingTxSyncer.sync_once(
                 history_fetcher: fn _ -> flunk("must not run") end,
                 raw_tx_fetcher: fn _ -> flunk("must not run") end,
                 processor: fn _ -> flunk("must not run") end
               )
    end
  end

  describe "sync_once/1 — backfill path" do
    test "backfills every new txid returned by the history fetcher" do
      _addr = insert_address!("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa")
      a = rand_txid_hex()
      b = rand_txid_hex()

      {processor, calls} = recording_processor()

      raw_tx_fetcher = fn hex -> {:ok, "deadbeef" <> hex} end

      assert %{addresses: 1, backfilled: 2, already_known: 0, errors: 0} =
               MissingTxSyncer.sync_once(
                 history_fetcher: fn _ -> {:ok, [a, b]} end,
                 raw_tx_fetcher: raw_tx_fetcher,
                 processor: processor
               )

      processed = calls.()
      assert length(processed) == 2

      # Verify the raw binary actually went through Base.decode16 — the
      # processor must see decoded bytes, not the hex string.
      assert Enum.all?(processed, &is_binary/1)
      assert Enum.any?(processed, fn b -> b == Base.decode16!("deadbeef" <> a, case: :mixed) end)
    end

    test "skips txids already in meta_transactions and counts them as :already_known" do
      _addr = insert_address!("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa")
      known = insert_known_tx!(rand_txid_hex())
      fresh = rand_txid_hex()

      {processor, calls} = recording_processor()

      assert %{backfilled: 1, already_known: 1, errors: 0} =
               MissingTxSyncer.sync_once(
                 history_fetcher: fn _ -> {:ok, [known, fresh]} end,
                 raw_tx_fetcher: fn _ -> {:ok, "ab" |> String.duplicate(50)} end,
                 processor: processor
               )

      # Only the fresh tx was processed
      assert length(calls.()) == 1
    end
  end

  describe "sync_once/1 — error counting" do
    test "counts a history fetcher error as one :error per address" do
      insert_address!("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa")
      insert_address!("1BvBMSEYstWetqTFn5Au4m4GFg7xJaNVN2")

      assert %{addresses: 2, errors: 2, backfilled: 0} =
               MissingTxSyncer.sync_once(
                 history_fetcher: fn _ -> {:error, :timeout} end,
                 raw_tx_fetcher: fn _ -> flunk("never reached") end,
                 processor: fn _ -> flunk("never reached") end
               )
    end

    test "counts a raw-tx fetcher error as :error and continues with the next txid" do
      insert_address!("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa")
      good = rand_txid_hex()
      bad = rand_txid_hex()

      raw_tx_fetcher = fn
        hex when hex == bad -> {:error, :not_found}
        _ -> {:ok, String.duplicate("ab", 50)}
      end

      {processor, calls} = recording_processor()

      assert %{backfilled: 1, errors: 1} =
               MissingTxSyncer.sync_once(
                 history_fetcher: fn _ -> {:ok, [bad, good]} end,
                 raw_tx_fetcher: raw_tx_fetcher,
                 processor: processor
               )

      assert length(calls.()) == 1
    end

    test "counts an invalid raw-hex response as :error and skips the processor" do
      insert_address!("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa")
      txid = rand_txid_hex()

      {processor, calls} = recording_processor()

      assert %{backfilled: 0, errors: 1} =
               MissingTxSyncer.sync_once(
                 history_fetcher: fn _ -> {:ok, [txid]} end,
                 raw_tx_fetcher: fn _ -> {:ok, "not-valid-hex!!"} end,
                 processor: processor
               )

      assert calls.() == []
    end

    test "counts a malformed history txid as :error and continues with the rest" do
      insert_address!("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa")
      good = rand_txid_hex()

      {processor, calls} = recording_processor()

      assert %{backfilled: 1, errors: 1} =
               MissingTxSyncer.sync_once(
                 history_fetcher: fn _ -> {:ok, ["not-a-txid!!", good]} end,
                 raw_tx_fetcher: fn _ -> {:ok, String.duplicate("ab", 50)} end,
                 processor: processor
               )

      assert length(calls.()) == 1
    end
  end

  describe "sync_once/1 — multiple addresses" do
    test "aggregates counts across all watched addresses" do
      insert_address!("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa")
      insert_address!("1BvBMSEYstWetqTFn5Au4m4GFg7xJaNVN2")

      shared_hex = rand_txid_hex()

      history_fetcher = fn
        "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa" -> {:ok, [rand_txid_hex(), shared_hex]}
        "1BvBMSEYstWetqTFn5Au4m4GFg7xJaNVN2" -> {:ok, [rand_txid_hex(), shared_hex]}
      end

      # The same shared_hex shows up in both histories. The FIRST appearance
      # backfills it (inserts a row would-be); the SECOND should find it
      # in meta_transactions and skip — but in this test the processor is
      # a no-op, so meta_transactions stays empty and both will backfill.
      # That confirms the "skip if already in DB" decision is rerun each
      # tick rather than memoized in process state.
      {processor, calls} = recording_processor()

      assert %{addresses: 2, backfilled: 4, errors: 0} =
               MissingTxSyncer.sync_once(
                 history_fetcher: history_fetcher,
                 raw_tx_fetcher: fn hex -> {:ok, String.duplicate("ab", 32) <> hex} end,
                 processor: processor
               )

      assert length(calls.()) == 4
    end
  end
end
