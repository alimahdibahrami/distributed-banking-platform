"""Internal two-phase commit coordinator. Only Gateway publishes a host port."""
from __future__ import annotations

import os
import asyncio
from contextlib import asynccontextmanager, suppress
from pathlib import Path

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from bank_platform.common.shard_map import load_shard_map
from bank_platform.common.validation import BankingError
from bank_platform.coordinator.engine import CoordinatorEngine, DecisionJournal
from bank_platform.shard.twopc import normalize_txid
from bank_platform.migration.routing_guard import assert_configuration

MAP = Path(__file__).resolve().parents[2] / 'config' / 'shard_map.json'


def create_app(*, journal: DecisionJournal | None = None,
               transport: httpx.AsyncBaseTransport | None = None) -> FastAPI:
    assert_configuration()
    db = journal or DecisionJournal(os.getenv('COORDINATOR_DB_PATH', '/app/state/coordinator.sqlite3'))

    @asynccontextmanager
    async def lifespan(app):
        db.initialize()
        async with httpx.AsyncClient(transport=transport, timeout=httpx.Timeout(6.0)) as client:
            engine = CoordinatorEngine(db, load_shard_map(os.getenv('SHARD_MAP_FILE', str(MAP))), client)
            app.state.engine = engine
            # Resolve all journal decisions from before this process started.
            # Unknown NEW records become durable ABORT, never COMMIT.
            await engine.recover_once(startup=True)
            monitor = asyncio.create_task(engine.watchdog(float(os.getenv('COORDINATOR_RECOVERY_INTERVAL', '3'))))
            try:
                yield
            finally:
                monitor.cancel()
                with suppress(asyncio.CancelledError):
                    await monitor
                app.state.engine = None

    app = FastAPI(title='Durable 2PC Coordinator', version='0.6.0', lifespan=lifespan)
    app.state.engine = None

    @app.get('/health')
    def health():
        return {'status': 'alive', 'service': 'coordinator',
                'transactions_ready': app.state.engine is not None,
                'routing_epoch': (app.state.engine.mapping.version
                                  if app.state.engine is not None else None)}

    @app.post('/internal/transactions')
    async def transaction(request: Request):
        if app.state.engine is None:
            return JSONResponse(status_code=503, content={'error': 'COORDINATOR_STARTING'})
        try:
            body = await request.json()
            if type(body) is not dict:
                raise BankingError('INVALID_REQUEST', 'Expected object', 400)
            code, result = await app.state.engine.transfer(body)
        except BankingError as exc:
            return JSONResponse(status_code=exc.http_status, content={'error': exc.code, 'message': exc.message})
        except (ValueError, TypeError):
            return JSONResponse(status_code=400, content={'error': 'INVALID_REQUEST'})
        return JSONResponse(status_code=code, content=result)

    @app.get('/internal/transactions/{transaction_id}')
    async def get_transaction(transaction_id: str):
        if app.state.engine is None:
            return JSONResponse(status_code=503, content={'error': 'COORDINATOR_STARTING'})
        try:
            normalized = normalize_txid(transaction_id)
        except BankingError as exc:
            return JSONResponse(status_code=exc.http_status, content={'error': exc.code})
        row = db.get(normalized)
        if row is None:
            return JSONResponse(status_code=404, content={'transaction_id': normalized, 'status': 'UNKNOWN', 'decision': 'UNKNOWN'})
        return app.state.engine.public_state(row)

    return app


app = create_app()
