# Phase 6 — Crash recovery and durable ABORT fencing

## Invariants and mechanism

1. `DecisionJournal` (SQLite, `synchronous=FULL`, persistent `coordinator_state` volume) records intent `NEW` before any participant PREPARE. Once a `COMMIT` is durable, recovery must **never** change it to `ABORT`.
2. At startup, coordinator scans persisted `NEW`, `COMMIT`, `ABORT`: a pre-decision `NEW` becomes **durable ABORT**, while durable decisions are redelivered until both participants acknowledge. A background watchdog retries undecided records older than one minute and all incomplete decisions every three seconds.
3. PostgreSQL prepared transactions hold locks through crash/restart; each participant uses `PREPARE TRANSACTION` / `COMMIT PREPARED` / `ROLLBACK PREPARED` plus an atomic commit receipt to detect idempotent COMMIT.
4. Each shard has a new durable `tpc_participant_aborts` tombstone table. A transaction-level advisory lock fences the race between **in-flight PREPARE** and **final ABORT**. When a PREPARE is in-flight, an ABORT decision returns `PENDING` (not a false acknowledgement); the watchdog retries after prepare completes. When acknowledged, any late PREPARE for that UUID is rejected. ABORT tombstones and COMMIT receipts persist across replica and PostgreSQL restart.
5. A transaction is `COMMITTED` only after both participants' acknowledgements; before that, it is `PENDING`, even with durable `COMMIT`. A COMMIT decision is not a license to reply success early.
6. One-shot local-operator fault markers (not publicly accessible through Gateway) crash only Coordinator with exit code 86, at deterministic points: `after-first-prepare`, `after-durable-commit`, `after-first-ack`.

## Live VPS acceptance

After deploying this Phase 6 code and migration:

```bash
cd /opt/distributed-banking-platform
docker compose config --quiet
# Mandatory schema migration: ensure both abort-fence tables exist even when
# Compose considers the old one-shot init jobs already completed.
docker compose run --rm --no-deps shard_a_init
docker compose run --rm --no-deps shard_b_init
docker compose --parallel 1 up -d --build
docker compose ps -a
python3 scripts/verify_phase2.py
python3 scripts/verify_phase3.py
python3 scripts/verify_phase4.py
python3 scripts/verify_phase5.py
python3 scripts/verify_phase6.py
```

The default Phase 6 preflight makes no deliberate service changes. The next command **deliberately kills the Coordinator three times** and manually restarts it, without touching PostgreSQL or Assignment 1:

```bash
python3 scripts/verify_phase6.py --crash-tests
```

The test uses only `A0802` and `A1802`, generates fresh UUIDv4 IDs and cleans up successful commits through distinct reverse transfers. Never automatically rerun on failure: inspect the returned transaction UUID, read `GET /api/v1/transactions/{uuid}`, inspect `docker compose logs --tail=100 coordinator`, and query PostgreSQL `pg_prepared_xacts`. Never run `docker compose down -v`, remove prepared GIDs, or delete the persistent Coordinator journal.

**Not included**: coordinator election/HA, live shard rebalancing, indefinite partition availability, or any guarantee under simultaneous loss of the Coordinator journal and PostgreSQL data volume.
