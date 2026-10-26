# Phase 2 acceptance (persistent sharding)

## Implemented

- Idempotent initialization jobs `shard_a_init` / `shard_b_init`, preserving Phase-1 Docker volumes.
- Tables `accounts` and `shard_metadata` in two isolated PostgreSQL instances.
- 1000 accounts per shard; no copied authoritative balance across shards.
- PostgreSQL CHECK constraints reject writing any account outside its shard's initial range.
- Two replicas per shard share the same PostgreSQL host, with separate HTTP processes.
- `GET /ready` fails when storage is unreachable / incorrect; `GET /health` is only liveness.
- `GET /internal/accounts/{account_id}` is a read-only verification endpoint.
- Mutation endpoints remain 501 until later phases.

## Local tests

Run `python -m pytest -q` (unit and static checks). These tests cannot substitute for Docker and PostgreSQL acceptance.

## Required live acceptance on VPS

1. Confirm the previous Assignment 1 stack still runs and no ports/volumes conflict.
2. Execute `docker compose config --quiet` with real `.env` values.
3. Execute `docker compose up -d --build` and `docker compose ps -a`.
4. Confirm both initialization jobs exited with code 0; all 4 replicas healthy.
5. Execute `python3 scripts/verify_phase2.py` (database distribution, wrong-shard constraints, 4 replica reads).
6. Execute `python3 scripts/verify_phase2.py --restart-check` (PostgreSQL container restarts, then re-read).
7. Confirm Assignment 1 is still running and its data/volumes are unchanged.

**Status: pending live runtime verification.** Do not mark Phase 2 completely accepted until all seven checks pass on the user's VPS.

## Current limitations

- Shard owner map is fixed A0001..A1000 / A1001..A2000. Live ownership migration comes in optional bonus.
- There is no request routing or load balancing yet; those are Phase 3.
- There is no deposit, withdraw, transfer, coordinator logic or 2PC yet.
- No auto-restart/self-healing; Docker restart policy stays `no`.
