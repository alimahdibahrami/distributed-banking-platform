# Phase 4 - Local banking safety

## Added

- Gateway accepts `balance`, `deposit`, `withdraw`, and same-shard `transfer`.
- A transfer request uses `{"action":"transfer","from_account":"A0701","to_account":"A0702","amount":10}`.
- Gateway resolves shard ownership before selecting a healthy replica.
- Replica checks ownership again; PostgreSQL row locks (`SELECT ... FOR UPDATE`) prevent lost updates between replica processes.
- Same-shard transfer holds both row locks in ascending account order and commits **both** account updates in one DB transaction; any SQL error rolls back both.
- Negative balances and 64-bit integer overflow are rejected; amount must be a positive integer.
- Backend identity and balance are not authoritative in replica memory.
- Gateway **never automatically retries a financial write after network failure**, even on a different healthy replica. Returns `UNKNOWN_OUTCOME (504)` because commit may already have happened.
- Cross-shard transfer returns `501 CROSS_SHARD_2PC_NOT_READY`. It is not a pair of sequential writes. Real 2PC is Phase 5.
- Persistent request-ID based deduplication, 2PC, transaction-status query, and coordinator recovery are NOT yet implemented. Clients should not manually retry a write after an unknown outcome.

## Local acceptance

```bash
python -m pytest -q
```

## VPS acceptance

The Phase 4 tests intentionally **mutate test balances** but restore them on successful completion. They avoid the instructor's A0500/A1500 example accounts.

```bash
cd /opt/distributed-banking-platform
docker compose config --quiet
docker compose --parallel 1 up -d --build
docker compose ps -a
python3 scripts/verify_phase2.py
python3 scripts/verify_phase3.py
python3 scripts/verify_phase4.py
```

Do not run `docker compose down -v`, do not delete any Volume, and do not deploy an old source archive after Phase 4.
If Phase 4 test reports `UNKNOWN_OUTCOME`, STOP and inspect account state and logs instead of automatically retrying.

## Acceptance gate

- Positive writes update the correct shard and are visible from both replicas.
- Concurrent deposit/withdraw on the same account have no lost updates.
- Same-shard transfers preserve total balance and either update both accounts or neither.
- Mixed transfers locking overlapping account rows do not deadlock.
- Cross-shard transfers remain disabled until real 2PC is implemented.
- Phase 2 and Phase 3 regression tests still pass.
