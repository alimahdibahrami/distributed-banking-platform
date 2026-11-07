# Phase 8A - truthful readiness reporting

- `GET /health` is process **liveness** only; it never claims the banking pipeline is ready.
- Bank replica `GET /ready` checks live PostgreSQL connectivity, shard identity and the expected account rows, and returns `banking_ready=true` only when those checks pass. Database failure returns HTTP 503 with `banking_ready=false`.
- Gateway `GET /ready` returns `banking_ready=true` and HTTP 200 **only if** each shard has >=1 healthy replica according to the Gateway routing monitor, **and** the durable 2PC Coordinator reports `transactions_ready=true` via its `/health` endpoint. The Coordinator probe times out or fails closed.
- Gateway reports `routing_ready`, `coordinator_ready`, `banking_ready`, and `healthy_replicas` independently. Local operations may still work while Coordinator is unavailable, but full cross-shard banking readiness is false.
- This checks availability and declared dependencies, not an actual monetary write; success does not guarantee every future transaction commits.
- No changes to persistent storage, write operations, transaction IDs, 2PC decisions, balances, ports, Docker volumes, or Compose restart policies.
- The historical Phase 1 smoke test is intentionally **not** run against the completed Phase 7 application; the existing Phase 3 test and runner are updated to no longer confuse liveness with readiness.

## Acceptance

Run `python -m pytest -q`, then on the VPS (only after staging source changes, build and controlled restart):

```bash
curl -fsS http://127.0.0.1:8001/health
curl -fsS http://127.0.0.1:8001/ready
python3 scripts/verify_phase3.py
python3 scripts/verify_phase6.py
```

Do not run destructive commands, remove volumes, or touch Assignment 1.
