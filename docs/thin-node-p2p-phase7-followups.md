# Thin-node P2P — Phase 7 follow-ups

Tracked work deferred out of Phase 6 (headers chain + reorg detection). These are
intentional scope boundaries, not omissions.

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
