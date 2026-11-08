"""Banking replica with PostgreSQL-backed atomic local operations."""
from __future__ import annotations

import logging
import os

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from starlette.concurrency import run_in_threadpool

from bank_platform.common.shard_map import account_number
from bank_platform.common.validation import BankingError, validate_local_operation, validate_transfer
from bank_platform.shard.storage import PostgresAccountStore, ShardIdentityError
from bank_platform.shard.twopc import Pg2pcParticipant, normalize_txid
from bank_platform.migration.epoch import strip_internal_epoch, stale_epoch_error
from bank_platform.migration.shard_cohort import (
    assert_cohort_configuration, cohort_required, configured_epoch,
)

LOGGER = logging.getLogger("uvicorn.error")  # Visible in Docker/Uvicorn logs


def create_app(
    shard_id: str | None = None,
    replica_id: str | None = None,
    store: PostgresAccountStore | None = None,
) -> FastAPI:
    shard = shard_id or os.getenv("SHARD_ID")
    replica = replica_id or os.getenv("REPLICA_ID")
    if shard not in ("A", "B", "C") or replica not in (f"{shard}1", f"{shard}2"):
        raise ValueError("Expected A/B/C shard and matching replica")
    if shard == "C" and os.getenv("BANK_ENABLE_SHARD_C", "0") != "1":
        raise ValueError("Shard C must be explicitly enabled in an isolated deployment")
    assert_cohort_configuration(bank=True)
    storage = store if store is not None else PostgresAccountStore(shard)
    app = FastAPI(title=f"Bank Replica {replica}", version="0.4.0")
    app.state.store = storage
    tpc = Pg2pcParticipant(storage)

    @app.get("/health")
    def health() -> dict:
        # Liveness is independent of storage and banking availability.
        return {"status": "alive", "service": "bank", "shard": shard,
                "replica": replica}

    @app.get("/ready")
    def ready():
        try:
            storage.ready()
            if cohort_required():
                epoch = configured_epoch()
                if not storage.route_proof(epoch):
                    raise ShardIdentityError('Shard DB routing epoch proof failed')
        except Exception:
            LOGGER.exception("Storage readiness failed on replica %s", replica)
            return JSONResponse(status_code=503, content={
                "status": "unavailable", "shard": shard, "replica": replica,
                "storage_ready": False, "banking_ready": False,
            })
        body = {"status": "ready", "shard": shard, "replica": replica,
                "storage_ready": True, "banking_ready": True}
        if cohort_required():
            body.update(routing_epoch=configured_epoch(), ownership_verified=True)
        return body

    @app.get("/internal/accounts/{account_id}")
    def read_account(account_id: str, owner_epoch: int | None = None):
        # Read ownership must be checked under a PostgreSQL fence row lock.
        # An old Gateway must never read B's stale copy after B -> C.
        try:
            account_number(account_id)
        except ValueError:
            return JSONResponse(status_code=400, content={"error": "INVALID_ACCOUNT"})
        if not storage.owns(account_id):
            return JSONResponse(status_code=409, content={"error": "WRONG_SHARD"})
        try:
            balance = (storage.read_account(account_id, owner_epoch=owner_epoch)
                       if cohort_required() else storage.read_account(account_id))
        except BankingError as exc:
            return JSONResponse(status_code=exc.http_status, content={"error": exc.code})
        except Exception:
            LOGGER.exception("Read failed on replica %s", replica)
            return JSONResponse(status_code=503, content={"error": "STORAGE_UNAVAILABLE"})
        if balance is None:
            return JSONResponse(status_code=404, content={"error": "ACCOUNT_NOT_FOUND"})
        LOGGER.info("bank.read account=%s shard=%s replica=%s", account_id, shard, replica)
        return {"account": account_id, "balance": balance, "shard": shard, "replica": replica}

    @app.post("/internal/operations")
    async def operations(request: Request):
        try:
            body = await request.json()
        except ValueError:
            return JSONResponse(status_code=400, content={"error": "INVALID_REQUEST"})
        try:
            body, owner_epoch = strip_internal_epoch(body)
            command = validate_local_operation(body)
            account_number(command.account)
        except BankingError as exc:
            return JSONResponse(status_code=exc.http_status, content={"error": exc.code})
        except ValueError:
            return JSONResponse(status_code=400, content={"error": "INVALID_ACCOUNT"})
        if not storage.owns(command.account):
            return JSONResponse(status_code=409, content={"error": "WRONG_SHARD"})
        try:
            if command.action == "balance":
                value = (storage.read_account(command.account, owner_epoch=owner_epoch)
                         if cohort_required() else storage.read_account(command.account))
                if value is None:
                    return JSONResponse(status_code=404, content={"error": "ACCOUNT_NOT_FOUND"})
            else:
                value = (storage.mutate(command.action, command.account, command.amount,
                                        owner_epoch=owner_epoch) if owner_epoch is not None
                         else storage.mutate(command.action, command.account, command.amount))
        except BankingError as exc:
            return JSONResponse(status_code=exc.http_status, content={"error": exc.code})
        except Exception as exc:
            fenced = stale_epoch_error(exc)
            if fenced:
                return JSONResponse(status_code=fenced.http_status, content={"error": fenced.code})
            LOGGER.exception("Local banking operation failed on %s", replica)
            return JSONResponse(status_code=503, content={"error": "STORAGE_UNAVAILABLE"})
        LOGGER.info("bank.operation action=%s account=%s shard=%s replica=%s", command.action, command.account, shard, replica)
        return {"action": command.action, "account": command.account,
                "balance": value, "shard": shard, "replica": replica}

    @app.post("/internal/transfers")
    async def transfer(request: Request):
        try:
            body = await request.json()
        except ValueError:
            return JSONResponse(status_code=400, content={"error": "INVALID_REQUEST"})
        try:
            body, owner_epoch = strip_internal_epoch(body)
            command = validate_transfer(body)
        except BankingError as exc:
            return JSONResponse(status_code=exc.http_status, content={"error": exc.code})
        if not (storage.owns(command.source) and storage.owns(command.destination)):
            return JSONResponse(status_code=409, content={"error": "WRONG_SHARD"})
        try:
            from_balance, to_balance = (
                storage.transfer_local(command.source, command.destination, command.amount,
                                       owner_epoch=owner_epoch) if owner_epoch is not None
                else storage.transfer_local(command.source, command.destination, command.amount))
        except BankingError as exc:
            return JSONResponse(status_code=exc.http_status, content={"error": exc.code})
        except Exception as exc:
            fenced = stale_epoch_error(exc)
            if fenced:
                return JSONResponse(status_code=fenced.http_status, content={"error": fenced.code})
            LOGGER.exception("Local transfer failed on %s", replica)
            return JSONResponse(status_code=503, content={"error": "STORAGE_UNAVAILABLE"})
        LOGGER.info("bank.local_transfer from=%s to=%s amount=%d shard=%s replica=%s",
                    command.source, command.destination, command.amount, shard, replica)
        return {"action": "transfer", "from_account": command.source, "to_account": command.destination,
                "from_balance": from_balance, "to_balance": to_balance,
                "shard": shard, "replica": replica}

    @app.post('/internal/tpc/prepare')
    async def tpc_prepare(request: Request):
        try:
            body = await request.json()
            body, owner_epoch = strip_internal_epoch(body)
            if type(body) is not dict or set(body) != {'transaction_id', 'account', 'role', 'amount'}:
                raise BankingError('INVALID_REQUEST', 'Incorrect 2PC prepare fields', 400)
            if owner_epoch is not None:
                body = {**body, 'owner_epoch': owner_epoch}
            state = await run_in_threadpool(tpc.prepare, **body)
        except BankingError as exc:
            return JSONResponse(status_code=exc.http_status, content={'error': exc.code})
        except (ValueError, TypeError, ShardIdentityError):
            return JSONResponse(status_code=400, content={'error': 'INVALID_REQUEST_OR_OWNER'})
        except Exception as exc:
            fenced = stale_epoch_error(exc)
            if fenced:
                return JSONResponse(status_code=fenced.http_status, content={'error': fenced.code})
            LOGGER.exception('2PC prepare storage failure on %s', replica)
            return JSONResponse(status_code=503, content={'error': 'PREPARE_UNCERTAIN'})
        LOGGER.info('2pc.prepare txid=%s shard=%s replica=%s state=%s', body['transaction_id'], shard, replica, state)
        return {'status': state, 'shard': shard, 'replica': replica}

    @app.post('/internal/tpc/decision')
    async def tpc_decision(request: Request):
        try:
            body = await request.json()
            if type(body) is not dict or set(body) != {'transaction_id', 'decision'}:
                raise BankingError('INVALID_REQUEST', 'Incorrect 2PC decision fields', 400)
            state = await run_in_threadpool(tpc.finalize, body['transaction_id'], body['decision'])
        except BankingError as exc:
            return JSONResponse(status_code=exc.http_status, content={'error': exc.code})
        except (ValueError, TypeError):
            return JSONResponse(status_code=400, content={'error': 'INVALID_REQUEST'})
        except Exception:
            LOGGER.exception('2PC finalize storage failure on %s', replica)
            return JSONResponse(status_code=503, content={'error': 'DECISION_UNCERTAIN'})
        LOGGER.info('2pc.decision txid=%s shard=%s replica=%s state=%s', body['transaction_id'], shard, replica, state)
        return {'status': state, 'shard': shard, 'replica': replica}

    @app.get('/internal/tpc/{transaction_id}')
    def tpc_status(transaction_id: str):
        try:
            state = tpc.inspect(transaction_id)
        except BankingError as exc:
            return JSONResponse(status_code=exc.http_status, content={'error': exc.code})
        except Exception:
            LOGGER.exception('2PC lookup failure on %s', replica)
            return JSONResponse(status_code=503, content={'error': 'STATUS_UNAVAILABLE'})
        return {'status': state, 'shard': shard, 'replica': replica}

    return app


if os.getenv("SHARD_ID") and os.getenv("REPLICA_ID"):
    app = create_app()
