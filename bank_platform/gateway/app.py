"""Shard-aware Gateway: balance reads route only to healthy owning replicas."""
from __future__ import annotations

import asyncio
import logging
import os
from uuid import uuid4
from contextlib import asynccontextmanager, suppress
from pathlib import Path

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from bank_platform.common.shard_map import ShardMap, load_shard_map
from bank_platform.common.validation import BankingError, validate_local_operation, validate_transfer
from bank_platform.gateway.routing import ReplicaRouter, poll_health
from bank_platform.migration.epoch import outgoing_epoch

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
            app.state.coordinator_client = client
            await router.refresh()
            monitor = asyncio.create_task(poll_health(router, interval))
            try:
                yield
            finally:
                monitor.cancel()
                with suppress(asyncio.CancelledError):
                    await monitor
                app.state.router = None
                app.state.coordinator_client = None

    app = FastAPI(title="Distributed Banking Gateway", version="0.4.0", lifespan=lifespan)
    app.state.shard_map = mapping
    app.state.router = None
    app.state.coordinator_client = None

    @app.get("/health")
    def health() -> dict:
        # Process liveness only: do not expose a stale banking readiness flag.
        return {"status": "alive", "service": "gateway"}

    @app.get("/ready")
    async def ready():
        router: ReplicaRouter | None = app.state.router
        counts = ({name: router.available(name) for name in router.replicas}
                  if router is not None else {shard.name: 0 for shard in mapping.shards})
        routing_ready = all(value > 0 for value in counts.values())
        coordinator_ready = False
        client: httpx.AsyncClient | None = app.state.coordinator_client
        if client is not None:
            try:
                endpoint = os.getenv("COORDINATOR_URL", "http://coordinator:8000").rstrip("/")
                response = await client.get(endpoint + "/health", timeout=2.0)
                if response.status_code == 200:
                    payload = response.json()
                    coordinator_ready = (
                        isinstance(payload, dict)
                        and payload.get("status") == "alive"
                        and payload.get("service") == "coordinator"
                        and payload.get("transactions_ready") is True
                    )
            except (httpx.HTTPError, ValueError, TypeError):
                # A failed dependency probe must never report fake readiness.
                pass
        banking_ready = routing_ready and coordinator_ready
        body = {"status": "ready" if banking_ready else "unavailable",
                "service": "gateway", "routing_ready": routing_ready,
                "coordinator_ready": coordinator_ready, "banking_ready": banking_ready,
                "healthy_replicas": counts}
        return JSONResponse(status_code=200 if banking_ready else 503, content=body)

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

    @app.get('/api/v1/transactions/{transaction_id}')
    async def transaction_status(transaction_id: str):
        from bank_platform.shard.twopc import normalize_txid
        try:
            txid = normalize_txid(transaction_id)
        except BankingError as exc:
            return error(exc.http_status, exc.code, exc.message)
        client = app.state.coordinator_client
        if client is None:
            return error(503, 'GATEWAY_STARTING', 'Gateway not ready')
        try:
            response = await client.get(
                os.getenv('COORDINATOR_URL', 'http://coordinator:8000').rstrip('/') + '/internal/transactions/' + txid,
                timeout=4.0)
            result = response.json()
            if response.status_code in (200, 404) and isinstance(result, dict):
                return JSONResponse(status_code=response.status_code, content=result)
        except (httpx.HTTPError, ValueError):
            pass
        return error(503, 'STATUS_UNAVAILABLE', 'Coordinator status temporarily unavailable')

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
                    client = app.state.coordinator_client
                    if client is None:
                        return error(503, "GATEWAY_STARTING", "Gateway has not completed startup")
                    # Caller can supply a stable UUIDv4 to safely repeat or query.
                    txid = raw.get('transaction_id') or str(uuid4())
                    request_body = {**raw, 'transaction_id': txid}
                    try:
                        response = await client.post(
                            os.getenv('COORDINATOR_URL', 'http://coordinator:8000').rstrip('/') + '/internal/transactions',
                            json=request_body, timeout=20.0)
                        data = response.json()
                    except (httpx.HTTPError, ValueError):
                        return JSONResponse(status_code=504, content={
                            'status': 'UNKNOWN_OUTCOME', 'transaction_id': txid,
                            'error': {'code': 'UNKNOWN_OUTCOME', 'message': 'Query transaction status; do not resubmit with a new ID'},
                        })
                    if not isinstance(data, dict):
                        return error(504, 'UNKNOWN_OUTCOME', 'Malformed coordinator reply')
                    if response.status_code == 200 and data.get('status') == 'COMMITTED':
                        return {'action': 'transfer', 'from_account': transfer.source,
                                'to_account': transfer.destination, 'amount': transfer.amount, **data}
                    if response.status_code == 202 and data.get('status') == 'PENDING':
                        return JSONResponse(status_code=202, content=data)
                    if response.status_code == 409:
                        return JSONResponse(status_code=409, content={
                            'status': data.get('status', 'error'), 'transaction_id': txid,
                            'decision': data.get('decision', 'UNKNOWN'),
                            'error': {'code': data.get('reason') or data.get('error', 'TRANSFER_REJECTED'),
                                      'message': 'Cross-shard transfer rejected'},
                        })
                    return JSONResponse(status_code=503, content={
                        'status': 'UNKNOWN_OUTCOME', 'transaction_id': txid,
                        'error': {'code': 'COORDINATOR_UNAVAILABLE', 'message': 'Query transaction ID before retry'},
                    })
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
        result = await router.write_once(shard, path, {**raw, **outgoing_epoch(mapping.version)})
        if result is None:
            return error(503, "NO_HEALTHY_REPLICA", f"No healthy replica in shard {shard.name}")
        code, payload = result
        if code == 200:
            return payload
        return error(code, payload.get("error", "OPERATION_FAILED"),
                     payload.get("message", payload.get("error", "Operation rejected")))


    return app


app = create_app()
