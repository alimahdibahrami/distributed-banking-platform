"""Banking replica scaffold. Replica owns no in-memory authoritative balance."""
from __future__ import annotations

import os

from fastapi import FastAPI
from fastapi.responses import JSONResponse


def create_app(shard_id: str | None = None, replica_id: str | None = None) -> FastAPI:
    shard = shard_id or os.getenv("SHARD_ID")
    replica = replica_id or os.getenv("REPLICA_ID")
    if shard not in ("A", "B") or replica not in (f"{shard}1", f"{shard}2"):
        raise ValueError("Expected SHARD_ID=A/B and matching REPLICA_ID=A1/A2/B1/B2")
    app = FastAPI(title=f"Bank Replica {replica}", version="0.1.0")

    @app.get("/health")
    def health() -> dict:
        return {"status": "alive", "service": "bank", "shard": shard,
                "replica": replica, "banking_ready": False}

    @app.post("/internal/operations")
    def operations() -> JSONResponse:
        return JSONResponse(status_code=501, content={
            "status": "error", "error": {"code": "NOT_IMPLEMENTED", "message": "Shard storage is not ready"}
        })

    return app


# Compose passes SHARD_ID and REPLICA_ID to each replica.
# Module import without these variables is intentionally invalid.
if os.getenv("SHARD_ID") and os.getenv("REPLICA_ID"):
    app = create_app()
