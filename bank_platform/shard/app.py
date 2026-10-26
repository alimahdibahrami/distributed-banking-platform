"""Banking replica: persistent shard-aware reads only in phase 2."""
from __future__ import annotations

import logging
import os

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from bank_platform.common.shard_map import account_number
from bank_platform.shard.storage import PostgresAccountStore

LOGGER = logging.getLogger(__name__)


def create_app(
    shard_id: str | None = None,
    replica_id: str | None = None,
    store: PostgresAccountStore | None = None,
) -> FastAPI:
    shard = shard_id or os.getenv("SHARD_ID")
    replica = replica_id or os.getenv("REPLICA_ID")
    if shard not in ("A", "B") or replica not in (f"{shard}1", f"{shard}2"):
        raise ValueError("Expected SHARD_ID=A/B and matching REPLICA_ID=A1/A2/B1/B2")
    storage = store if store is not None else PostgresAccountStore(shard)
    app = FastAPI(title=f"Bank Replica {replica}", version="0.2.0")
    app.state.store = storage

    @app.get("/health")
    def health() -> dict:
        # Liveness intentionally does not imply banking functionality.
        return {"status": "alive", "service": "bank", "shard": shard,
                "replica": replica, "banking_ready": False}

    @app.get("/ready")
    def ready():
        try:
            storage.ready()
        except Exception:
            LOGGER.exception("Storage readiness failed on replica %s", replica)
            return JSONResponse(status_code=503, content={
                "status": "unavailable", "shard": shard, "replica": replica,
                "storage_ready": False,
            })
        return {"status": "ready", "shard": shard, "replica": replica,
                "storage_ready": True, "banking_ready": False}

    @app.get("/internal/accounts/{account_id}")
    def read_account(account_id: str):
        # Internal, read-only probe. Gateway does not yet expose balance lookup.
        try:
            account_number(account_id)
        except ValueError:
            return JSONResponse(status_code=400, content={"error": "INVALID_ACCOUNT"})
        if not storage.owns(account_id):
            return JSONResponse(status_code=409, content={"error": "WRONG_SHARD"})
        try:
            balance = storage.read_account(account_id)
        except Exception:
            LOGGER.exception("Read failed on replica %s", replica)
            return JSONResponse(status_code=503, content={"error": "STORAGE_UNAVAILABLE"})
        if balance is None:
            return JSONResponse(status_code=404, content={"error": "ACCOUNT_NOT_FOUND"})
        return {"account": account_id, "balance": balance, "shard": shard, "replica": replica}

    @app.post("/internal/operations")
    def operations() -> JSONResponse:
        return JSONResponse(status_code=501, content={
            "status": "error", "error": {
                "code": "NOT_IMPLEMENTED", "message": "Banking operations are not ready"
            }
        })

    return app


if os.getenv("SHARD_ID") and os.getenv("REPLICA_ID"):
    app = create_app()
