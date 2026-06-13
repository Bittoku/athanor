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

    test "chain_tip_p2p_active?/1 requires the route PRIMARY to be :p2p (override respected, note 963 B1)" do
      # An operator override making RPC the primary chain-tip authority with P2P as
      # a fallback must NOT read as P2P-active, even with peers live — otherwise the
      # RPC-primary override is silently disabled.
      Application.put_env(:athanor, Athanor.P2P.SourceRouter,
        routes: %{chain_tip: {:rpc, [:p2p]}}
      )

      on_exit(fn -> Application.delete_env(:athanor, Athanor.P2P.SourceRouter) end)

      refute ChainTipVerifier.chain_tip_p2p_active?(p2p_available?: true)
    end

    test "should_defer_to_p2p?/2 is false while suspended even if peers are live (blocker 2)" do
      assert ChainTipVerifier.should_defer_to_p2p?(false, p2p_available?: true)
      refute ChainTipVerifier.should_defer_to_p2p?(true, p2p_available?: true)
      refute ChainTipVerifier.should_defer_to_p2p?(false, p2p_available?: false)
    end

    # Hermes !18 note 945 B1: the RPC poll must keep catching up the local→seed gap
    # before it hands authority to P2P.
    test "defer_to_p2p?/4 does NOT defer until the local index reaches the P2P seed/root height" do
      # P2P active + not suspended, but local height below the P2P root → RPC stays
      # the authority and keeps catching up the gap.
      refute ChainTipVerifier.defer_to_p2p?(false, 90, 100, p2p_available?: true)
      # Caught up to (or past) the seed → defer to P2P.
      assert ChainTipVerifier.defer_to_p2p?(false, 100, 100, p2p_available?: true)
      assert ChainTipVerifier.defer_to_p2p?(false, 150, 100, p2p_available?: true)
    end

    test "defer_to_p2p?/4 does not defer when the P2P chain is unseeded (root height unknown)" do
      refute ChainTipVerifier.defer_to_p2p?(false, 100, nil, p2p_available?: true)
    end

    test "defer_to_p2p?/4 still respects suspension and availability" do
      refute ChainTipVerifier.defer_to_p2p?(true, 150, 100, p2p_available?: true)
      refute ChainTipVerifier.defer_to_p2p?(false, 150, 100, p2p_available?: false)
    end
  end

  # Hermes !18 note 937: when RPC is the active tip authority it must reconcile by
  # HASH (not just height), and recover from the common ancestor.
  describe "reconcile_plan/4 (RPC-authority reorg detection by hash)" do
    test "empty local index → catch up from height 1" do
      assert ChainTipVerifier.reconcile_plan(0, 5, fn _ -> nil end, fn _ -> "ab" end) ==
               {:catch_up, 1, 5}
    end

    test "equal heights with a matching tip hash → synced" do
      same = fn _ -> "aa" end
      assert ChainTipVerifier.reconcile_plan(5, 5, same, same) == :synced
    end

    test "behind with a matching tip → forward catch-up (no rollback)" do
      local = fn h -> %{5 => "aa"}[h] end
      node = fn h -> %{5 => "aa", 6 => "bb", 7 => "cc"}[h] end
      assert ChainTipVerifier.reconcile_plan(5, 7, local, node) == {:catch_up, 6, 7}
    end

    test "equal heights but DIVERGENT tip hash → reorg to the common ancestor (blocker 1)" do
      # Agree at height 3; diverge at 4 and 5. Same height, different hashes.
      local = fn h -> %{3 => "c", 4 => "ld4", 5 => "ld5"}[h] end
      node = fn h -> %{3 => "c", 4 => "nd4", 5 => "nd5"}[h] end
      assert ChainTipVerifier.reconcile_plan(5, 5, local, node) == {:reorg, 3, 5}
    end

    test "orphaned local tip with the node ahead → reorg from the common ancestor (blocker 2)" do
      # Common ancestor 3; the connect range therefore starts at 4, so the
      # canonical block at the old local height (5) is reprocessed, not skipped.
      local = fn h -> %{3 => "c", 4 => "ld4", 5 => "ld5"}[h] end
      node = fn h -> %{3 => "c", 4 => "nd4", 5 => "nd5", 6 => "nd6"}[h] end
      assert ChainTipVerifier.reconcile_plan(5, 6, local, node) == {:reorg, 3, 6}
    end
  end

  describe "reconcile/3 execution (RPC authority)" do
    test "a divergence dispatches ONE apply_reorg: rollback to ancestor + canonical branch from ancestor+1" do
      shared = "AA"
      local = fn h -> %{3 => shared, 4 => "B4", 5 => "B5"}[h] end
      node = fn h -> %{3 => shared, 4 => "C4", 5 => "C5"}[h] end

      assert {:reorg, 3, 5} =
               ChainTipVerifier.reconcile(5, 5,
                 local_hash_at: local,
                 node_hash_at: node,
                 processor: self(),
                 batch: 10
               )

      c4 = Base.decode16!("C4")
      c5 = Base.decode16!("C5")
      assert_received {:"$gen_cast", {:apply_reorg, 3, [^c4, ^c5]}}
      refute_received {:"$gen_cast", {:process_block_hash, _}}
    end

    test "behind dispatches forward catch-up casts only (no rollback)" do
      local = fn h -> %{5 => "AA"}[h] end
      node = fn h -> %{5 => "AA", 6 => "B6", 7 => "B7"}[h] end

      assert {:catch_up, 6, 7} =
               ChainTipVerifier.reconcile(5, 7,
                 local_hash_at: local,
                 node_hash_at: node,
                 processor: self(),
                 batch: 10
               )

      b6 = Base.decode16!("B6")
      b7 = Base.decode16!("B7")
      assert_received {:"$gen_cast", {:process_block_hash, ^b6}}
      assert_received {:"$gen_cast", {:process_block_hash, ^b7}}
      refute_received {:"$gen_cast", {:apply_reorg, _, _}}
    end
  end

  # Hermes !18 note 941: the RPC reconcile must positively prove the ancestor and
  # never dispatch on missing/sparse hashes.
  describe "reconcile robustness (note 941)" do
    test "blocker 1: defers (no rollback) when a hash below the tip is unknown — ancestor not proven" do
      # Tip hashes differ (looks like a reorg), but the node hash below the tip is
      # unavailable (RPC pruned/transient) so the common ancestor can't be proven.
      local = fn h -> %{5 => "B5", 4 => "B4", 3 => "AA"}[h] end
      node = fn h -> %{5 => "C5"}[h] end

      assert :defer =
               ChainTipVerifier.reconcile(5, 5,
                 local_hash_at: local,
                 node_hash_at: node,
                 processor: self(),
                 batch: 10
               )

      refute_received {:"$gen_cast", {:apply_reorg, _, _}}
      refute_received {:"$gen_cast", {:process_block_hash, _}}
    end

    test "blocker 1: a local hash gap below the tip also defers (no destructive rollback)" do
      local = fn h -> %{5 => "B5"}[h] end
      node = fn h -> %{5 => "C5", 4 => "C4", 3 => "AA"}[h] end

      assert :defer =
               ChainTipVerifier.reconcile(5, 5,
                 local_hash_at: local,
                 node_hash_at: node,
                 processor: self(),
                 batch: 10
               )

      refute_received {:"$gen_cast", {:apply_reorg, _, _}}
    end

    test "blocker 2: catch-up dispatches only the contiguous prefix, stopping at the first gap" do
      local = fn h -> %{5 => "AA"}[h] end
      # node tip 8 but height 7 is missing.
      node = fn h -> %{5 => "AA", 6 => "C6", 8 => "C8"}[h] end

      assert {:catch_up, 6, 8} =
               ChainTipVerifier.reconcile(5, 8,
                 local_hash_at: local,
                 node_hash_at: node,
                 processor: self(),
                 batch: 10
               )

      c6 = Base.decode16!("C6")
      assert_received {:"$gen_cast", {:process_block_hash, ^c6}}
      # Neither the gap (7) nor anything after it is dispatched.
      refute_received {:"$gen_cast", {:process_block_hash, _}}
    end

    test "blocker 2: catch-up defers entirely when the first canonical hash is missing" do
      local = fn h -> %{5 => "AA"}[h] end
      node = fn h -> %{5 => "AA", 7 => "C7"}[h] end

      assert :defer =
               ChainTipVerifier.reconcile(5, 7,
                 local_hash_at: local,
                 node_hash_at: node,
                 processor: self(),
                 batch: 10
               )

      refute_received {:"$gen_cast", {:process_block_hash, _}}
    end

    test "blocker 2: reorg connects only the contiguous canonical prefix from ancestor+1" do
      local = fn h -> %{5 => "B5", 4 => "B4", 3 => "AA"}[h] end
      # ancestor 3; node tip 8 with height 7 missing.
      node = fn h -> %{5 => "C5", 4 => "C4", 3 => "AA", 6 => "C6", 8 => "C8"}[h] end

      assert {:reorg, 3, 8} =
               ChainTipVerifier.reconcile(5, 8,
                 local_hash_at: local,
                 node_hash_at: node,
                 processor: self(),
                 batch: 10
               )

      connect = Enum.map(["C4", "C5", "C6"], &Base.decode16!/1)
      assert_received {:"$gen_cast", {:apply_reorg, 3, ^connect}}
    end
  end
end
