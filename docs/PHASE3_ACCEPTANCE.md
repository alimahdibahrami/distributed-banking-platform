# Phase 3 - Gateway routing and replica failover

## Scope

- Shard owner is resolved **first** from the versioned Shard Map; routing never sends a request to another shard.
- Inside the owning shard, `/ready` determines healthy replicas; `Round Robin` spreads reads across them.
- Gateway polls each replica every 2 seconds (configurable using `GATEWAY_HEALTH_INTERVAL`), removes unhealthy replicas, and readmits restored ones.
- Read-only failed upstream requests are retried against **another healthy replica in the same shard**. This is safe for reads; mutating operations remain 501 and are never retried.
- Gateway and individual replicas log selected shard and replica for load-distribution evidence.
- `GET /api/v1/accounts/{account_id}/balance` and `POST /api/v1/operations` with `{"action":"balance","account":"A0500"}` expose read-only account inquiry through the Gateway.
- `GET /health` is *liveness*, not banking readiness. `GET /ready` is **routing readiness**: requires >=1 healthy replica in every shard. `banking_ready=false` until local writes and transfers are implemented.

## Local checks

```bash
python -m pip install -r requirements-dev.txt
python -m pytest -q
```

## Live checks on Docker Compose VPS

Keep Assignment 1 on port 8000; Gateway is bound only to `127.0.0.1:8001`.

```bash
docker compose config --quiet
docker compose --parallel 1 up -d --build
docker compose ps -a
python3 scripts/verify_phase2.py
python3 scripts/verify_phase3.py
python3 scripts/verify_phase3.py --failover
docker compose ps -a
docker compose logs --tail=50 gateway bank_a1 bank_a2 bank_b1 bank_b2
```

`--failover` **temporarily stops only `bank_a1`** while concurrent read traffic is running. The test recovers that replica in a `finally` block, confirms its return, and checks that the unrelated shard remained available. It does not stop DB containers or touch Assignment 1. Do not run the failure test when banking loads are production-critical.

## Evidence/gate

1. `verify_phase2.py` still passes (no regression).
2. 80 requests for A0500 alternate between A1/A2; 80 for A1500 alternate B1/B2; no cross-shard route; invalid accounts are rejected.
3. Under injected failure of bank_a1, A2 continues to serve read requests; Gateway marks A1 unhealthy, stops selecting it, then includes it after rejoin.
4. `GET /ready` becomes 503 if either shard has zero healthy replicas while `/health` remains 200. (Covered by local tests; live test isolates one replica only.)
5. Logs show selected shard/replica and old Assignment 1 remains healthy.

**Status:** Code and local tests ready. Live VPS acceptance is pending until the user runs the commands and supplies the output.

## Limitations

- Gateway runs as one process/instance. In-memory Round Robin position and health state reset at restart; this does not affect authoritative balances.
- A stopped replica is *not* automatically replaced in phase 3; automatic replacement is optional Self-Healing.
- All bank writes (deposit, withdrawal, transfers) await phases 4-6. Only read requests are safe to retry transparently.
