# Docker Hub release — Assignment 2

## Verified artifact

- Image: `docker.io/alimahdibahrami/distributed-banking-platform`
- Tag published: `phase8a-20261107`
- Digest: `sha256:0e08efd6aa5c92e0bb84d6691f7e5a0aa2f610d5622fded464c3ec8e2ebda29b`
- Confirmed by operator: `docker push` and `docker manifest inspect`.
- Source commit at readiness release: `4feca72`.

`compose.release.yaml` is a standalone copy of `compose.yaml`, with the **only** service changes being removal of local `build:` and pinning the six application services (Gateway, Coordinator, 4 replicas) to the exact public Docker Hub digest. Database and bootstrap services still use `postgres:17-alpine`. The database image tag is **not** digest-pinned, so strict bit-for-bit database-image reproducibility is not claimed.

## On a clean host only

1. Obtain the complete source repository (including `database/shard_a.sql`, `database/shard_b.sql`, and `config/`).
2. Create local `.env` based on `.env.example` and configure the required database passwords. Never commit `.env`.
3. Check the configuration and planned images:

   ```sh
   docker compose -f compose.release.yaml config --quiet
   docker compose -f compose.release.yaml config --images
   ```

4. Pull the published application image and official PostgreSQL image:

   ```sh
   docker compose -f compose.release.yaml pull
   ```

5. For a **clean, dedicated host with no existing deployment**, start the platform:

   ```sh
   docker compose -f compose.release.yaml up -d --wait
   curl -fsS http://127.0.0.1:8001/ready
   ```

   Default bind is `127.0.0.1:8001`; it is not exposed to the Internet.

## Safety: existing VPS

The operational VPS already runs the project with named persistent volumes and an Assignment 1 app. **Do not execute `up`, `down -v`, `pull`, `prune`, or `--force-recreate` as part of release validation on the live VPS.** To validate release metadata in place, run **only**:

```sh
docker compose -f compose.release.yaml config --quiet
docker compose -f compose.release.yaml config --images
docker manifest inspect docker.io/alimahdibahrami/distributed-banking-platform:phase8a-20261107 >/dev/null
docker compose ps
```

The first two commands parse local configuration; the last two inspect remote image and existing containers. They do not deploy.

## Evidence and acceptance

- Full seven-experiment run: `7/7 PASS` on VPS.
- Phase 8A readiness: `/ready` returns `routing_ready=true`, `coordinator_ready=true`, `banking_ready=true`.
- Post-deployment Phase 5 and Experiment 7: PASS.
- Keep Phase 7 evidence archive separate from `.env`, which contains secrets.

## File integrity

See `SHA256SUMS` and compare it with `Get-FileHash` (Windows) or `sha256sum` (Linux).
