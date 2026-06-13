defmodule Athanor.Workers.ChainTipVerifierTest do
  @moduledoc """
  Tests for `Athanor.Workers.ChainTipVerifier.apply_tip_event/1` (Phase 6 T6.3,
  §C) — the bridge that turns the pure `HeadersChain` tip decisions into the
  existing block-processing + rollback machinery.

  Covers, after the Hermes !18 review (note 932):

    * `{:extend}` enqueues new blocks to the processor.
    * `{:reorg}` routes rollback + new-branch enqueue as a **single ordered**
      `BlockProcessor` mailbox op (`{:apply_reorg, fork_height, connect}`), never a
      direct rollback racing the cast queue (blocker 3).
    * `{:reorg_too_deep}` suspends P2P tip authority so the RPC poll resumes even
      while peers stay live; a healthy `{:extend}`/`{:reorg}` resumes it (blocker 2).
    * the authority gate (`chain_tip_p2p_active?/1`, `should_defer_to_p2p?/2`).

  Block work is observed through an injected `:processor` (a `cast` lands as a
  message) so no DB/RPC is needed; tip-event hashes are **display order**. The
  Repo-backed fork-height lookup and the actual serialized rollback get their own
  `DataCase` coverage (the fork-height test here + `block_processor_reorg_test`).
  """
  use Athanor.DataCase, async: false

  alias Athanor.Schema.BlockProcessContext
  alias Athanor.Workers.ChainTipVerifier

  defp display_hash(byte), do: :binary.copy(<<byte>>, 32)

  describe "apply_tip_event/2 — {:extend, …}" do
    test "enqueues each new block hash to the processor, in order" do
      h1 = display_hash(0xA1)
      h2 = display_hash(0xA2)

      assert :ok =
               ChainTipVerifier.apply_tip_event({:extend, [h1, h2]},
                 processor: self(),
                 verifier: self()
               )

      assert_received {:"$gen_cast", {:process_block_hash, ^h1}}
      assert_received {:"$gen_cast", {:process_block_hash, ^h2}}
    end
  end

  describe "apply_tip_event/2 — {:reorg, …} (serialized via the processor mailbox)" do
    test "routes rollback + new branch as one ordered apply_reorg cast (no direct rollback)" do
      orphan = [display_hash(0xB2), display_hash(0xB1)]
      connect = [display_hash(0xC1), display_hash(0xC2)]

      assert :ok =
               ChainTipVerifier.apply_tip_event(
                 {:reorg, %{orphan: orphan, connect: connect}},
                 processor: self(),
                 verifier: self(),
                 resolve_height: fn ^orphan -> {:ok, 41} end
               )

      # A single ordered mailbox op carries BOTH the rollback height and the new
      # branch — rollback can never race the per-block casts.
      assert_received {:"$gen_cast", {:apply_reorg, 41, ^connect}}
      refute_received {:"$gen_cast", {:process_block_hash, _}}
    end

    test "with no known orphan heights, apply_reorg carries a nil fork height" do
      connect = [display_hash(0xC1)]

      assert :ok =
               ChainTipVerifier.apply_tip_event(
                 {:reorg, %{orphan: [display_hash(0xB1)], connect: connect}},
                 processor: self(),
                 verifier: self(),
                 resolve_height: fn _ -> :unknown end
               )

      assert_received {:"$gen_cast", {:apply_reorg, nil, ^connect}}
    end

    test "derives the fork height from orphan blocks recorded in block_process_contexts" do
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
                 processor: self(),
                 verifier: self()
               )

      assert_received {:"$gen_cast", {:apply_reorg, 41, []}}
    end
  end

  describe "apply_tip_event/2 — {:reorg_too_deep, …} (blocker 2: hand authority back to RPC)" do
    test "suspends P2P tip authority (casts to the verifier) and enqueues nothing" do
      assert :ok =
               ChainTipVerifier.apply_tip_event(
                 {:reorg_too_deep, %{rounds: 3}},
                 processor: self(),
                 verifier: self()
               )

      assert_received {:"$gen_cast", {:suspend_p2p_authority, %{rounds: 3}}}
      refute_received {:"$gen_cast", {:process_block_hash, _}}
      refute_received {:"$gen_cast", {:apply_reorg, _, _}}
    end

    test "a healthy extend/reorg resumes P2P authority" do
      assert :ok =
               ChainTipVerifier.apply_tip_event({:extend, []},
                 processor: self(),
                 verifier: self()
               )

      assert_received {:"$gen_cast", :resume_p2p_authority}
    end
  end

  describe "suspend/resume authority (GenServer state)" do
    test "the suspend/resume casts flip p2p_authority_suspended" do
      pid = start_supervised!(ChainTipVerifier)
      refute :sys.get_state(pid).p2p_authority_suspended

      ChainTipVerifier.apply_tip_event({:reorg_too_deep, %{rounds: 4}}, verifier: pid)
      assert :sys.get_state(pid).p2p_authority_suspended

      ChainTipVerifier.apply_tip_event({:extend, []}, processor: self(), verifier: pid)
      refute :sys.get_state(pid).p2p_authority_suspended
    end
  end

  describe "tip authority gate" do
    test "chain_tip_p2p_active?/1 reflects the :chain_tip route's P2P availability" do
      assert ChainTipVerifier.chain_tip_p2p_active?(p2p_available?: true)
      refute ChainTipVerifier.chain_tip_p2p_active?(p2p_available?: false)
    end

    test "should_defer_to_p2p?/2 is false while suspended even if peers are live (blocker 2)" do
      assert ChainTipVerifier.should_defer_to_p2p?(false, p2p_available?: true)
      refute ChainTipVerifier.should_defer_to_p2p?(true, p2p_available?: true)
      refute ChainTipVerifier.should_defer_to_p2p?(false, p2p_available?: false)
    end
  end
end
