# Phase 5 -- native PostgreSQL two-phase commit

## Implemented contract

- `POST /api/v1/operations` accepts a cross-shard transfer. Optional canonical
  UUIDv4 `transaction_id`; a new UUID is generated when absent. **For retry-safe
  clients supply their own ID and reuse it.**
- `GET /api/v1/transactions/{transaction_id}` returns `COMMITTED`, `ABORTED`,
  `PENDING` or `UNKNOWN` (404). Never interpret `PENDING` as a success.
- Coordinator stores transaction intent before any prepare and a COMMIT/ABORT
  decision in SQLite with `synchronous=FULL` before notifying participants.
  Existing `coordinator_state` Docker volume is preserved.
- Participants use REAL PostgreSQL `PREPARE TRANSACTION`, followed by
  `COMMIT PREPARED` or `ROLLBACK PREPARED`. The committed receipt in each shard
  is in the same prepared transaction as its account change. `max_prepared_transactions=100`.
- All prepare steps take shard locks in **A then B** order to reduce distributed
  deadlocks on opposing transfers. Local transfers still use row lock ordering.
- Only after **both** participant commit acknowledgements does the Coordinator
  report `COMMITTED`; otherwise it records durable COMMIT and returns `PENDING`.
- A definitive negative vote triggers durable ABORT and rollback of both participants.
  An uncertain PREPARE response records durable ABORT but remains `PENDING`
  until Phase 6 reconciliation (no false completed-ABORT reply).
- Each shard has 2 app replicas using its shared persistent PostgreSQL database.
  Internal 2PC endpoints are not published to the host; only Gateway is.
- Phase 6 will add automatic crash recovery, watchdog, and fault-injection gates.
  In Phase 5 a crashed coordinator after PREPARE can leave pending row locks:
  **do not delete its SQLite volume or manually resolve prepared GIDs**.

## VPS acceptance (not run in local sandbox)

```bash
cd /opt/distributed-banking-platform
docker compose config --quiet
docker compose --parallel 1 up -d --build
docker compose ps -a
python3 scripts/verify_phase2.py
python3 scripts/verify_phase3.py
python3 scripts/verify_phase4.py
python3 scripts/verify_phase5.py
```

The first run applies the idempotent `tpc_participant_commits` schema migration.
`coordinator_state_init` is an additional one-shot job, using the already-present
Postgres image to set the existing coordinator volume ownership to the non-root
UID 10001. Successful verifier confirms debit/credit, ABORT, repeat IDs,
transaction-ID conflict, reverse transfer and no orphaned prepared transactions.

On `202 PENDING`, `504 UNKNOWN_OUTCOME`, any invariant failure, or unexpected
`pg_prepared_xacts`, **STOP and inspect state**. Never run `docker compose down -v`.
Use `GET /api/v1/transactions/{id}` and provide logs. This is not yet the
crash-recovery acceptance test; Phase 6 owns that gate.

Tests run locally: `python -m pytest -q`. PostgreSQL 2PC integration requires
live VPS since the authoring environment has no Docker daemon/PostgreSQL.
