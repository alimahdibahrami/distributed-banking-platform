# Implementation plan and acceptance gates

Source of truth: `Assignment2_Distributed_Banking_Platform.pdf`, sections 1-9.
No GitHub Issues. One or more scoped commits per phase; each phase is accepted only after testing.

## Phase 1 - Bootstrap (accepted on VPS)

- Independent Python package and source layout; no Assignment 1 data carried over.
- Two database service definitions with persistent volumes, four replica definitions,
  one Gateway, one Coordinator, Compose name `distributed-banking`.
- Deterministic read-only shard map with complete assignment account coverage.
- Reusable A1 validation rules and adapted Python client.
- Tests for service identity, HTTP liveness, stub safety, sharding boundaries and static Compose shape.
- Gate: Python unit tests pass, Compose config validated on a Docker host, containers start
  and expose expected liveness; no accidental interaction with Assignment 1.
- VPS execution confirmed: all services healthy; Assignment 1 remains separate.

## Phase 2 - Persistent sharding (accepted on VPS)

- Two idempotent SQL initialization jobs run against both fresh and existing volumes.
- 1000 distinct accounts per shard, with database-level range CHECK constraints.
- A0500=1000 and A1500=500 as in the instructor example; existing balances never overwritten.
- Persistent shard identity metadata, verified before reading any account.
- Each replica has PostgreSQL-backed read-only account lookup; no in-memory balance.
- At Phase 2 completion, Gateway reads were still 501; Phase 3 adds read-only routing. Banking writes remain Phase 4.
- Gate: on VPS run `python3 scripts/verify_phase2.py --restart-check` and collect proof
  of direct DB placement, read from A1/A2/B1/B2, rejection of wrong-shard insert,
  and durability after database container restart.
- VPS proof confirmed: Phase 2 isolation and PostgreSQL restart persistence both PASS.

## Phase 3 - Gateway and replica routing (implemented; live gate pending)

- Resolve account owner using Shard Map, then healthy replica (Round Robin).
- Poll replica `/ready`; fail over **read-only** requests to healthy replicas of the same shard.
- Expose Gateway balance inquiry, `/ready` and logs of selected replica.
- All mutations are intentionally 501 until Phase 4.
- Gate: run `python3 scripts/verify_phase3.py` and `--failover` on VPS; demonstrate experiments 1, 2 and 4.

## Phase 4 - Local banking safety

- Reuse A1 API validation semantics for balance/deposit/withdraw.
- ACID PostgreSQL transactions and explicit concurrency strategy.
- Transfer within one shard uses a single atomic transaction.
- Gate: concurrency/rollback/insufficient-funds tests pass; same-shard transfer atomic.

## Phase 5 - Real cross-shard 2PC

- Durable unique transaction IDs and coordinator states.
- PostgreSQL PREPARE TRANSACTION (not two sequential committed updates).
- Record COMMIT/ABORT decision before notifications; safely repeat decisions.
- Never acknowledge success without all participant COMMIT acknowledgements.
- Gate: cross-shard transfer preserves sums and correctly aborts on rejected vote.

## Phase 6 - Crash recovery

- Coordinator durable journal, unresolved status lookup (PENDING/UNKNOWN).
- Recover from failed vote (A), stop after PREPARE (B), stop after durable COMMIT (C).
- Preserve a durable COMMIT across temporary participant outage.
- Gate: deterministic process-stop/restart proofs for A/B/C.

## Phase 7 - Mandatory verification

- 7/7 instructor experiments, real multi-replica concurrency including overlapping
  cross-shard accounts, no lost updates, invariant checks, captured logs.
- Gate: reproducible test suite and artifacts for every experiment.

## Phase 8 - Release and hand-in

- Check existing VPS services/ports/volumes first; keep Assignment 1 untouched.
- Publish source and required Docker Hub images; deploy new Compose project.
- Report <=10 A4 pages, 2PC sequence diagram; demo video <=15 minutes.

## Optional bonus stages (start only after mandatory acceptance)

9. Self-Healing (+10%): custom replica controller, replacement containers, gateway
   registration, controller crash recovery without duplicates, tests under load;
   no Kubernetes/Swarm/prebuilt healers/Docker restart policies.
10. Live Re-Sharding (+12%): shard C (DB+2 replicas), snapshot and durable ordered
   change capture, idempotent replay, monotonic map epoch, write-path fencing,
   cutover integrated with 2PC, crash-consistent migration checkpoints.

## Original assignment baseline

- A1: 38 pytest tests passed before building this scaffold.
- Imported ideas/code: local request validation (adapted), Python CLI (adapted),
  FastAPI service/HTTP test conventions, non-root Docker image.
- Not imported: A1 SQLite balance DB, single-server service, Docker volumes,
  VPS upgrade scripts, legacy seed accounts.
- A1's three original seed accounts (A1001/A1002/A1003) are NOT imported;
  Phase 2 defines an independent 2000-account fixture across PostgreSQL A/B.

Progress accounting: completion is acceptance-gate based, not lines of code.
