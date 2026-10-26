"""Shard-aware Gateway: balance reads route only to healthy owning replicas."""
from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager, suppress
from pathlib import Path

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from bank_platform.common.shard_map import ShardMap, load_shard_map
from bank_platform.common.validation import BankingError, validate_local_operation, validate_transfer
from bank_platform.gateway.routing import ReplicaRouter, poll_health

DEFAULT_MAP_PATH = Path(__file__).resolve().parents[2] / "config" / "shard_map.json"
LOGGER = logging.getLogger(__name__)


def error(status: int, code: str, message: str) -> JSONResponse:
    return JSONResponse(status_code=status, content={
        "status": "error", "error": {"code": code, "message": message}
    })


def create_app(
    shard_map: ShardMap | None = None,
    *,
    transport: httpx.AsyncBaseTransport | None = None,
    poll_interval: float | None = None,
) -> FastAPI:
    mapping = shard_map or load_shard_map(os.getenv("SHARD_MAP_FILE", str(DEFAULT_MAP_PATH)))
    interval = poll_interval if poll_interval is not None else float(os.getenv("GATEWAY_HEALTH_INTERVAL", "2"))
    if interval <= 0:
        raise ValueError("Gateway health interval must be positive")

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        async with httpx.AsyncClient(transport=transport, timeout=httpx.Timeout(2.5)) as client:
            router = ReplicaRouter(mapping, client)
            app.state.router = router
            await router.refresh()
            monitor = asyncio.create_task(poll_health(router, interval))
            try:
                yield
            finally:
                monitor.cancel()
                with suppress(asyncio.CancelledError):
                    await monitor
                app.state.router = None

    app = FastAPI(title="Distributed Banking Gateway", version="0.4.0", lifespan=lifespan)
    app.state.shard_map = mapping
    app.state.router = None

    @app.get("/health")
    def health() -> dict:
        # Liveness is deliberately distinct from full banking readiness.
        return {"status": "alive", "service": "gateway", "banking_ready": False}

    @app.get("/ready")
    def ready():
        router: ReplicaRouter | None = app.state.router
        counts = ({name: router.available(name) for name in router.replicas}
                  if router is not None else {shard.name: 0 for shard in mapping.shards})
        good = all(value > 0 for value in counts.values())
        body = {"status": "ready" if good else "unavailable", "service": "gateway",
                "routing_ready": good, "banking_ready": False, "healthy_replicas": counts}
        return JSONResponse(status_code=200 if good else 503, content=body)

    async def balance(account_id: str):
        try:
            shard = mapping.owner_for(account_id)
        except ValueError:
            return error(400, "INVALID_ACCOUNT", "Account ID must be A0001..A2000")
        router: ReplicaRouter | None = app.state.router
        if router is None:
            return error(503, "GATEWAY_STARTING", "Gateway has not completed startup")
        result = await router.read_balance(account_id, shard)
        if result is None:
            return error(503, "NO_HEALTHY_REPLICA", f"No available replica in shard {shard.name}")
        if result.get("not_found"):
            return error(404, "ACCOUNT_NOT_FOUND", "Account does not exist")
        return result

    @app.get("/api/v1/accounts/{account_id}/balance")
    async def get_balance(account_id: str):
        return await balance(account_id)

    @app.post("/api/v1/operations")
    async def operations(request: Request):
        try:
            raw = await request.json()
        except ValueError:
            return error(400, "INVALID_REQUEST", "Expected JSON request body")
        try:
            if isinstance(raw, dict) and raw.get("action") == "transfer":
                transfer = validate_transfer(raw)
                first = mapping.owner_for(transfer.source)
                second = mapping.owner_for(transfer.destination)
                if first.name != second.name:
                    return error(501, "CROSS_SHARD_2PC_NOT_READY", "Cross-shard transfers need Phase 5 coordinator")
                shard, path = first, "/internal/transfers"
            else:
                command = validate_local_operation(raw)
                shard = mapping.owner_for(command.account)
                if command.action == "balance":
                    return await balance(command.account)
                path = "/internal/operations"
        except BankingError as exc:
            return error(exc.http_status, exc.code, exc.message)
        except ValueError:
            return error(400, "INVALID_ACCOUNT", "Account ID must be A0001..A2000")
        router: ReplicaRouter | None = app.state.router
        if router is None:
            return error(503, "GATEWAY_STARTING", "Gateway has not completed startup")
        result = await router.write_once(shard, path, raw)
        if result is None:
            return error(503, "NO_HEALTHY_REPLICA", f"No healthy replica in shard {shard.name}")
        code, payload = result
        if code == 200:
            return payload
        return error(code, payload.get("error", "OPERATION_FAILED"),
                     payload.get("message", payload.get("error", "Operation rejected")))


    return app


app = create_app()
