"""Gateway process scaffold: API implementation is intentionally pending."""
from __future__ import annotations

import os
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from bank_platform.common.shard_map import ShardMap, load_shard_map

DEFAULT_MAP_PATH = Path(__file__).resolve().parents[2] / "config" / "shard_map.json"


def create_app(shard_map: ShardMap | None = None) -> FastAPI:
    mapping = shard_map or load_shard_map(os.getenv("SHARD_MAP_FILE", str(DEFAULT_MAP_PATH)))
    app = FastAPI(title="Distributed Banking Gateway", version="0.1.0")
    app.state.shard_map = mapping

    @app.get("/health")
    def health() -> dict:
        return {"status": "alive", "service": "gateway", "banking_ready": False}

    @app.post("/api/v1/operations")
    def operations() -> JSONResponse:
        # Must never return success until database-backed routing is implemented.
        return JSONResponse(status_code=501, content={
            "status": "error", "error": {"code": "NOT_IMPLEMENTED", "message": "Banking operations are not ready"}
        })

    return app


app = create_app()
