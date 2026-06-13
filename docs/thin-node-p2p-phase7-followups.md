# Thin-node P2P — Phase 7 follow-ups

Tracked work deferred out of Phase 6 (headers chain + reorg detection). These are
intentional scope boundaries, not omissions.

## Scope boundary (why Phase 6 was split)

Phase 6 ships the native-Elixir **headers chain + reorg detection**: track the best
tip by cumulative work, validate per-header PoW against the network pow-limit, detect
reorgs by the common-ancestor walk, and **surface** tip events (`{:extend}` /
`{:reorg}` / `{:reorg_too_deep}`) to an `:on_tip` sink — which in Phase 6 **logs**
them (operator visibility). The existing RPC `ChainTipVerifier` poll remains the sole
tip authority for the index, unchanged.

**Driving the index from P2P tip events** — `ChainTipVerifier.apply_tip_event/1`, the
RPC↔P2P tip-authority handoff, the no-gap recovery invariant, and the bootstrap
boundary — proved to be a coherent design problem in its own right (it drew seven
review rounds of incremental patching on MR !18, all in this one subsystem, while the
headers-chain core stayed stable). It is therefore carved out into **F7.2** below to
be *designed* holistically rather than reverse-engineered from review comments.

## F7.1 — Full consensus difficulty / retarget validation for P2P headers

**Source:** Hermes review of MR !18, note 945 blocker 2.

**Current Phase 6 behaviour.** `Athanor.P2P.HeadersChain.Work.valid_pow?/3` validates
each header's proof of work by checking that its compact `bits`:

1. decodes to a target,
2. is at or below the network **pow-limit** (`Network.pow_limit`), and
3. is met by the header hash (`hash ≤ target`).

The pure tree then credits cumulative work for any header passing that per-header
gate. This is the gate specified in the merged Phase 6 plan
(`docs/thin-node-p2p-phase6-tasks.md`).

**Gap.** A peer can advertise a branch whose `bits` are *easier than the
consensus-required target for those heights* but still below the network pow-limit.
Such headers are not a valid BSV chain, yet they can pass the per-header gate and
participate in cumulative-work tip selection (and so trigger extend/reorg
decisions). Because `BlockProcessor.apply_reorg/3` rolls back before connecting and
the canonical branch is then fetched from the RPC node (which would reject the
invalid blocks), the practical impact is bounded to **unnecessary rollback churn**
rather than persistent corruption — but the P2P tip selector should not credit an
invalid-difficulty chain in the first place.

**Required for F7.1 (either approach):**

- **(a) Full DAA validation** — validate each header's `bits` against the expected
  network difficulty/retarget rules for its height, including the BSV
  difficulty-adjustment algorithm and any testnet-specific minimum-difficulty
  rules. This requires tracking the timestamps/heights needed by the DAA window.
- **(b) Advisory-until-confirmed** — keep P2P-selected header branches *advisory*
  (used only to prompt a `getheaders`/RPC reconciliation) and treat the RPC node as
  the authority that confirms a candidate branch before it can drive a rollback.

**Acceptance:** a header with valid `bits ≤ pow_limit` whose hash meets its own
claimed target, but whose difficulty is below the consensus-required value for its
height, is rejected and cannot win cumulative-work tip selection.

**Status:** deferred to Phase 7 (decision recorded 2026-06-13). Phase 6 ships with
the pow-limit gate; this hardens it to full consensus rules.

## F7.2 — P2P-driven chain-tip index integration (RPC↔P2P authority handoff + no-gap recovery)

**Source:** Hermes review of MR !18, notes 932/937/941/945/963/979/994 (the index-
integration thread). Split out of Phase 6 on 2026-06-13.

**Goal.** Drive the indexer from the Phase 6 headers chain: replace the detection-only
`:on_tip` logger with a bridge that applies `{:extend}`/`{:reorg}`/`{:reorg_too_deep}`
to the `BlockProcessor`, and reconcile the RPC poll with it under a single tip
authority. Design this as one coherent unit (it was the subsystem that resisted
incremental patching).

**Sub-requirements (each surfaced as an MR !18 review blocker; design them together):**

1. **Serialized reorg application** — rollback + new-branch enqueue as one ordered
   `BlockProcessor` mailbox op, never racing in-flight block casts (note 932 B3).
2. **RPC reconcile by hash** — when RPC is the tip authority, reconcile to the common
   ancestor by hash (not height): detect same-height divergence, recover the complete
   canonical branch from `ancestor+1`, and **defer** (never deep-rollback) on an
   unproven/unknown ancestor (notes 937, 941).
3. **Contiguous, no-gap connect** — process only a contiguous canonical prefix,
   halting at the first missing/invalid hash; never record a child over a missing
   predecessor (notes 941, 945, 979 B2).
4. **`last_height` integrity after rollback** — reflect the fork height immediately
   after a successful rollback, even on an empty/failed connect (note 941 B3).
5. **Deep-reorg fallback actually hands authority to RPC** — a `{:reorg_too_deep}`
   suspends P2P tip authority so the RPC poll resumes even with live peers (note 937 B2).
6. **Router-primary authority** — `chain_tip_p2p_active?` honours a `:chain_tip` route
   override whose primary is `:rpc` (note 963 B1). *(The route + `chain_tip_p2p_active?`
   predicate design lands here; the `:chain_tip` route remains declared in `SourceRouter`
   in Phase 6 as part of the §A capability registry, with no consumer until F7.2.)*
7. **Solicited-only detached escalation** — already implemented in the Phase 6
   `HeadersChain` (per-peer solicited gate, notes 963/979 B1); the *consumer* side
   (suspending index authority) lands here.
8. **RPC↔P2P seed handoff + durable extend** — keep RPC authoritative until the local
   index reaches the P2P tip; **buffer/replay** deferred P2P extends rather than
   dropping them, so the index can't stall at the seed (note 994 B1).
9. **Bootstrap boundary** — replace the `index_empty?()` missing-predecessor exception
   with an explicit bootstrap mode / configured checkpoint (with predecessor
   semantics), so no producer (P2P, ZMQ, JungleBus, RPC) can record an arbitrary high
   block as a non-contiguous first context (note 994 B2).

**Acceptance:** the full index-integration test matrix from the MR !18 thread, plus
the two unaddressed note-994 regressions: (a) local-below-seed extend deferred → RPC
catches local to seed → the same extension is applied without a new peer announcement;
(b) an empty index refuses a high block (e.g. height 105 with no height-104 context).

**Status:** deferred to Phase 7 (decision recorded 2026-06-13). The implementation
that existed on MR !18 (the reverted `ChainTipVerifier` + `BlockProcessor` integration)
is preserved in branch history at commit `5472b2b` as the starting point.
