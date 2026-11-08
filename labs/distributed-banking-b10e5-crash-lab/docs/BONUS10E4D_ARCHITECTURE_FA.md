# Design rationale — Bonus 10E-4D

1. **Baseline**: original 2000 accounts remain unchanged on the original VPS. A dedicated Compose project creates its own 2000 seeded accounts across A and B and an empty C database. Passwords are randomized on the target VPS.
2. **Snapshot**: enable capture then snapshot from B under a repeatable-read snapshot. Target C holds immutable snapshot baselines and per-account revision receipts in staging tables.
3. **Writes**: 80 concurrent local transfers between unique moved accounts produce 160 captured account revisions while preserving total money. The source continues banking operations during snapshot/catch-up.
4. **Fence**: physically serialized bank_migration_fence row flips to owner C, epoch 2 on source B; prepared transactions retain SHARE locks. No post-fence old-owner writes succeed. Verify stale/forged epoch SQL rejection.
5. **Convergence**: finish replay, prove staged 500-row digest equals fenced 500-row source digest, and assert coordinator pending and prepared transactions are zero.
6. **Promote**: write `ACTIVATED` intent before touching C/B. In one C transaction flip target fence and copy all staged rows to live accounts; verify revision+balance digest. In one B transaction preserve committed 2PC receipts, remove old owner accounts and shrink identity. Recovery checks authoritative B/C state and may safely resume if interrupted between transactions.
7. **Publish**: atomic swap of lab-only map file; set B range 1001-1500 and epoch 2; recreate the lab's 6 bank replicas, coordinator, gateway. The old Gateway/Coordinator may temporarily disagree: strict routing makes it fail closed. This is a controlled restart, not transparent online rolling upgrade.
8. **Postcheck**: prove Gateway A/B/C routing, 6 ready replicas, account counts 1000/500/500, combined sum 1,999,500, source rows 0, 2PC A↔C and B↔C, and persisted journal COMPLETED.

## Production blockers

A truly non-disruptive cutover needs a dual-version Gateway deployment with an atomic external traffic switch, an explicit cutover authorization barrier for new cross-shard 2PC, a coordinated read/write draining protocol, fault-injection evidence at all crash gates, and roll-forward/recovery proofs across full container restarts. The current control plane and source-only code are not sufficient grounds for performing live money migration.
