# Implementation plan and acceptance gates

Source of truth: `Assignment2_Distributed_Banking_Platform.pdf`, sections 1-9.
No GitHub Issues. One or more scoped commits per phase; each phase is accepted only after testing.

## Phase 1 - Bootstrap (this package)

- Independent Python package and source layout; no Assignment 1 data carried over.
- Two database service definitions with persistent volumes, four replica definitions,
  one Gateway, one Coordinator, Compose name `distributed-banking`.
- Deterministic read-only shard map with complete assignment account coverage.
- Reusable A1 validation rules and adapted Python client.
- Tests for service identity, HTTP liveness, stub safety, sharding boundaries and static Compose shape.
- Gate: Python unit tests pass, Compose config validated on a Docker host, containers start
  and expose expected liveness; no accidental interaction with Assignment 1.
- IMPORTANT: Runtime Docker/Compose validation is pending until deployment on VPS.

## Phase 2 - Persistent sharding

- PostgreSQL schema, migrations/initialization, shard-owned accounts and seed data.
- Ensure no account has authoritative state on both shards.
- Real PostgreSQL access from both replicas of a shard.
- Gate: direct DB inspection proves correct placement and durable restart behavior.

## Phase 3 - Gateway and replica routing

- Resolve account owner using Shard Map, then healthy replica (Round Robin).
- Implement health checks and fail selection; requests cannot go to unrelated shard.
- Logs identify the selected replica for load distribution evidence.
- Gate: experiments 1, 2 and 4 pass under live Docker.

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
- A1's three original seed accounts (A1001/A1002/A1003) are NOT the
  full 2-shard fixture required for A2; seed data will be defined in Phase 2.

Progress accounting: completion is acceptance-gate based, not lines of code.
