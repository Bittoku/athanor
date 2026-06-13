defmodule Athanor.Workers.ChainTipVerifierTest do
  @moduledoc """
  Tests for `Athanor.Workers.ChainTipVerifier.apply_tip_event/1` (Phase 6 T6.3,
  §C) — the bridge that turns the pure `HeadersChain` tip decisions into the
  existing block-processing + rollback machinery, plus the `:chain_tip` authority
  gate that lets the RPC poll defer to P2P when P2P is the active tip source.

  Block-processing is exercised through an injected `:processor` (so a `cast` lands
  as a message we can assert) and an injected `:rollback`; the Repo-backed
  fork-height lookup gets one `DataCase` test. Tip-event hashes are **display
  order** (as `HeadersChain` emits them).
  """
  use Athanor.DataCase, async: true

  alias Athanor.Schema.BlockProcessContext
  alias Athanor.Workers.ChainTipVerifier

  defp display_hash(byte), do: :binary.copy(<<byte>>, 32)

  describe "apply_tip_event/2 — {:extend, …}" do
    test "enqueues each new block hash to the processor, in order" do
      h1 = display_hash(0xA1)
      h2 = display_hash(0xA2)

      assert :ok =
               ChainTipVerifier.apply_tip_event({:extend, [h1, h2]}, processor: self())

      assert_received {:"$gen_cast", {:process_block_hash, ^h1}}
      assert_received {:"$gen_cast", {:process_block_hash, ^h2}}
    end
  end

  describe "apply_tip_event/2 — {:reorg, …}" do
    test "rolls back to the common-ancestor height then enqueues the new branch" do
      test = self()

      orphan = [display_hash(0xB2), display_hash(0xB1)]
      connect = [display_hash(0xC1), display_hash(0xC2)]

      assert :ok =
               ChainTipVerifier.apply_tip_event(
                 {:reorg, %{orphan: orphan, connect: connect}},
                 processor: self(),
                 rollback: fn height -> send(test, {:rolled_back, height}) end,
                 resolve_height: fn ^orphan -> {:ok, 41} end
               )

      assert_received {:rolled_back, 41}
      c1 = display_hash(0xC1)
      c2 = display_hash(0xC2)
      assert_received {:"$gen_cast", {:process_block_hash, ^c1}}
      assert_received {:"$gen_cast", {:process_block_hash, ^c2}}
    end

    test "with no known orphan heights, applies the new branch without rolling back" do
      test = self()
      connect = [display_hash(0xC1)]

      assert :ok =
               ChainTipVerifier.apply_tip_event(
                 {:reorg, %{orphan: [display_hash(0xB1)], connect: connect}},
                 processor: self(),
                 rollback: fn height -> send(test, {:rolled_back, height}) end,
                 resolve_height: fn _ -> :unknown end
               )

      refute_received {:rolled_back, _}
      c1 = display_hash(0xC1)
      assert_received {:"$gen_cast", {:process_block_hash, ^c1}}
    end

    test "derives the fork height from orphan blocks recorded in block_process_contexts" do
      test = self()
      now = DateTime.utc_now() |> DateTime.truncate(:second)

      orphan42 = display_hash(0xD2)
      orphan43 = display_hash(0xD3)

      for {hash, height} <- [{orphan42, 42}, {orphan43, 43}] do
        Repo.insert!(%BlockProcessContext{
          id: Base.encode16(hash, case: :lower),
          height: height,
          processed_at: now
        })
      end

      # Default (Repo-backed) resolve_height: min(42,43) − 1 = 41.
      assert :ok =
               ChainTipVerifier.apply_tip_event(
                 {:reorg, %{orphan: [orphan43, orphan42], connect: []}},
                 rollback: fn height -> send(test, {:rolled_back, height}) end
               )

      assert_received {:rolled_back, 41}
    end
  end

  describe "apply_tip_event/2 — {:reorg_too_deep, …}" do
    test "is a no-op (no rollback, no enqueue): the RPC poll stays the authority" do
      test = self()

      assert :ok =
               ChainTipVerifier.apply_tip_event(
                 {:reorg_too_deep, %{rounds: 3}},
                 processor: self(),
                 rollback: fn height -> send(test, {:rolled_back, height}) end
               )

      refute_received {:rolled_back, _}
      refute_received {:"$gen_cast", {:process_block_hash, _}}
    end
  end

  describe "chain_tip_p2p_active?/1 (tip authority via SourceRouter)" do
    test "true when the :chain_tip route's P2P primary is available" do
      assert ChainTipVerifier.chain_tip_p2p_active?(p2p_available?: true)
    end

    test "false when P2P is unavailable — the RPC poll remains the tip authority (cold-start parity)" do
      refute ChainTipVerifier.chain_tip_p2p_active?(p2p_available?: false)
    end
  end
end
