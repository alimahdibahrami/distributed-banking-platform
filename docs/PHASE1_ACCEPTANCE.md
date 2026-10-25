# Phase 1 acceptance checklist

## Tested locally

- A1 baseline: 38 existing tests passed in the original unmodified source tree.
- A2 unit/structure tests: 39 tests passed on 2026-10-25.
- Live ASGI smoke: started Gateway, one bank replica and Coordinator using Uvicorn;
  all returned `GET /health` 200 and reported readiness=false.
- Gateway POST /api/v1/operations returned 501 as designed.
- YAML parsed and exactly 8 expected service names detected.
- `python -m compileall` completed without errors.

## Pending on the user's VPS (required to close Phase 1)

1. Copy project to a NEW directory, not the Assignment 1 directory.
2. In that new directory run `sh scripts/preflight_vps.sh` and check port 8001.
3. Copy `.env.example` to `.env` and set private database passwords.
4. Run `docker compose config --quiet` (actual Docker CLI validation).
5. Run `docker compose up -d --build` and inspect `docker compose ps`.
6. Run `python scripts/smoke_phase1.py` on the VPS host.
7. Run `docker compose exec shard_a_db pg_isready -U bank -d bank_a` and
   equivalent for B; inspect all 4 bank container health values.
8. Confirm the original `mini-banking` container and named volumes still exist
   and its former `/health` still responds on its existing endpoint.

Only after these checks is Phase 1 complete. No stateful money operation should
be attempted against the Phase 1 API (it is intentionally unavailable).

Never run `docker compose down -v` during any migration or demonstration.
