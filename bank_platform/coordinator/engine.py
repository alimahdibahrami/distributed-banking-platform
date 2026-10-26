"""Durable 2PC coordinator with startup recovery and periodic redelivery.

A COMMIT is irrevocable after the SQLite FULL-sync decision is recorded.
A recovered undecided NEW transaction must be ABORTED, never presumed COMMIT.
"""
from __future__ import annotations

import asyncio
import logging
import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path

import httpx

from bank_platform.common.shard_map import ShardMap
from bank_platform.common.validation import BankingError, validate_transfer
from bank_platform.shard.twopc import normalize_txid

LOGGER = logging.getLogger('uvicorn.error')


class DecisionJournal:
    def __init__(self, path: str):
        self.path = path

    @contextmanager
    def _connect(self):
        con = sqlite3.connect(self.path, timeout=10)
        try:
            con.row_factory = sqlite3.Row
            con.execute('PRAGMA synchronous=FULL')
            with con:
                yield con
        finally:
            con.close()

    def initialize(self):
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as con:
            con.execute('''CREATE TABLE IF NOT EXISTS transactions (
                transaction_id TEXT PRIMARY KEY,
                source TEXT NOT NULL, destination TEXT NOT NULL,
                amount INTEGER NOT NULL, state TEXT NOT NULL,
                a_ack INTEGER NOT NULL DEFAULT 0,
                b_ack INTEGER NOT NULL DEFAULT 0,
                reason TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )''')

    def incomplete(self, *, stale_new: bool = False, limit: int = 100) -> list[dict]:
        """Snapshot only durable, incomplete work (bounded per watchdog pass)."""
        states = "state IN ('NEW', 'COMMIT', 'ABORT')" if stale_new else "state IN ('COMMIT', 'ABORT')"
        age = " AND (state != 'NEW' OR created_at <= datetime('now', '-60 seconds'))" if stale_new else ''
        with self._connect() as con:
            return [dict(row) for row in con.execute(
                f'SELECT * FROM transactions WHERE {states}{age} ORDER BY created_at LIMIT ?', (limit,)
            ).fetchall()]

    def recoverable_on_start(self, *, limit: int = 10000) -> list[dict]:
        with self._connect() as con:
            return [dict(row) for row in con.execute(
                "SELECT * FROM transactions WHERE state IN ('NEW','COMMIT','ABORT') ORDER BY created_at LIMIT ?", (limit,)
            ).fetchall()]

    def get(self, transaction_id: str) -> dict | None:
        with self._connect() as con:
            row = con.execute('SELECT * FROM transactions WHERE transaction_id=?', (transaction_id,)).fetchone()
            return dict(row) if row is not None else None

    def create(self, txid: str, source: str, destination: str, amount: int) -> bool:
        with self._connect() as con:
            result = con.execute('INSERT OR IGNORE INTO transactions (transaction_id,source,destination,amount,state) VALUES (?,?,?,?,?)', (txid, source, destination, amount, 'NEW'))
            return result.rowcount == 1

    def decide(self, txid: str, decision: str, reason: str = ''):
        assert decision in ('COMMIT', 'ABORT')
        with self._connect() as con:
            result = con.execute("UPDATE transactions SET state=?, reason=? WHERE transaction_id=? AND state='NEW'", (decision, reason, txid))
            if result.rowcount != 1:
                raise RuntimeError('Decision already recorded; cannot overwrite durable 2PC decision')
        # Connection context COMMIT + synchronous=FULL before any phase-2 notification.

    def acknowledge(self, txid: str, shard: str):
        column = 'a_ack' if shard == 'A' else 'b_ack'
        with self._connect() as con:
            con.execute(f'UPDATE transactions SET {column}=1 WHERE transaction_id=?', (txid,))
            row = con.execute('SELECT state,a_ack,b_ack FROM transactions WHERE transaction_id=?', (txid,)).fetchone()
            if row and row['a_ack'] and row['b_ack']:
                if row['state'] == 'COMMIT':
                    con.execute("UPDATE transactions SET state='DONE' WHERE transaction_id=?", (txid,))
                elif row['state'] == 'ABORT':
                    con.execute("UPDATE transactions SET state='ABORTED' WHERE transaction_id=?", (txid,))


class CoordinatorEngine:
    def __init__(self, journal: DecisionJournal, mapping: ShardMap, client: httpx.AsyncClient):
        self.journal, self.mapping, self.client = journal, mapping, client
        self._locks: dict[str, asyncio.Lock] = {}

    async def _participant(self, shard: str, method: str, path: str, payload: dict | None = None) -> tuple[int, dict] | None:
        """Probe healthy replicas; never retry ambiguous PREPARE on a peer."""
        group = next(s.replicas for s in self.mapping.shards if s.name == shard)
        selected = None
        for endpoint in group:
            try:
                ready = await self.client.get(endpoint.rstrip('/') + '/ready')
                body = ready.json()
                if ready.status_code == 200 and body.get('shard') == shard and body.get('storage_ready') is True:
                    selected = endpoint.rstrip('/')
                    break
            except (httpx.HTTPError, ValueError):
                pass
        if selected is None:
            return None
        try:
            if method == 'GET':
                response = await self.client.get(selected + path)
            else:
                response = await self.client.post(selected + path, json=payload)
            body = response.json()
            if not isinstance(body, dict):
                return None
            return response.status_code, body
        except (httpx.HTTPError, ValueError):
            return None

    async def _prepare(self, shard: str, txid: str, account: str, role: str, amount: int) -> tuple[bool, str]:
        outcome = await self._participant(shard, 'POST', '/internal/tpc/prepare', {
            'transaction_id': txid, 'account': account, 'role': role, 'amount': amount,
        })
        if outcome is None:
            return False, 'UNCERTAIN_PREPARE'
        code, body = outcome
        if code == 200 and body.get('status') in ('PREPARED', 'COMMITTED'):
            return True, ''
        if code in (400, 404, 409) and isinstance(body.get('error'), str):
            return False, body['error']
        return False, 'UNCERTAIN_PREPARE'

    def _inject_crash(self, txid: str, gate: str) -> None:
        """Operator-only one-shot fault injection via coordinator volume.

        The Gateway never provides an API to arm this marker. A privileged
        operator must put an exact UUID marker in the coordinator state volume.
        """
        marker = Path(self.journal.path).parent / 'faults' / f'{txid}.{gate}'
        if marker.is_file():
            marker.unlink()  # consume before process exit; survives restarts
            LOGGER.error('coordinator.fault_injected txid=%s gate=%s', txid, gate)
            os._exit(86)

    async def recover_once(self, *, startup: bool = False) -> None:
        rows = (self.journal.recoverable_on_start() if startup
                else self.journal.incomplete(stale_new=True))
        for snapshot in rows:
            txid = snapshot['transaction_id']
            async with self._locks.setdefault(txid, asyncio.Lock()):
                record = self.journal.get(txid)
                if record is None:
                    continue
                if record['state'] == 'NEW':
                    # No durable COMMIT exists: only a durable ABORT is safe.
                    self.journal.decide(txid, 'ABORT', 'RECOVERED_UNDECIDED')
                    LOGGER.warning('coordinator.recovered_new txid=%s decision=ABORT', txid)
                if self.journal.get(txid)['state'] in ('COMMIT', 'ABORT'):
                    await self._deliver(txid)

    async def watchdog(self, interval: float = 3.0) -> None:
        while True:
            try:
                await self.recover_once(startup=False)
            except asyncio.CancelledError:
                raise
            except Exception:
                LOGGER.exception('coordinator.recovery_sweep_failed')
            await asyncio.sleep(interval)

    async def _deliver(self, txid: str) -> dict:
        row = self.journal.get(txid)
        if row['state'] not in ('COMMIT', 'ABORT'):
            return row
        # Outcomes are safe to repeat: COMMIT PREPARED has durable receipt.
        for shard in ('A', 'B'):
            if row[shard.lower() + '_ack']:
                continue
            response = await self._participant(shard, 'POST', '/internal/tpc/decision', {
                'transaction_id': txid, 'decision': row['state'],
            })
            if response is not None and response[0] == 200 and response[1].get('status') == ('COMMITTED' if row['state'] == 'COMMIT' else 'ABORTED'):
                self.journal.acknowledge(txid, shard)
                if shard == 'A' and row['state'] == 'COMMIT':
                    self._inject_crash(txid, 'after-first-ack')
            else:
                LOGGER.warning('coordinator.pending txid=%s shard=%s decision=%s', txid, shard, row['state'])
        return self.journal.get(txid)

    @staticmethod
    def public_state(row: dict) -> dict:
        state = row['state']
        return {
            'transaction_id': row['transaction_id'],
            'status': 'COMMITTED' if state == 'DONE' else 'ABORTED' if state == 'ABORTED' else 'PENDING',
            'decision': 'COMMIT' if state in ('COMMIT', 'DONE') else 'ABORT' if state in ('ABORT', 'ABORTED') else 'UNKNOWN',
            'acknowledged_shards': [s for s in ('A', 'B') if row[s.lower() + '_ack']],
            'reason': row['reason'],
        }

    async def transfer(self, body: dict) -> tuple[int, dict]:
        command = validate_transfer(body)
        if 'transaction_id' not in body:
            raise BankingError('INVALID_REQUEST', 'transaction_id required at coordinator', 400)
        txid = normalize_txid(body['transaction_id'])
        source_owner = self.mapping.owner_for(command.source).name
        target_owner = self.mapping.owner_for(command.destination).name
        if source_owner == target_owner:
            raise BankingError('SAME_SHARD', 'Coordinator accepts cross-shard transfers only', 400)
        async with self._locks.setdefault(txid, asyncio.Lock()):
            newly_created = self.journal.create(txid, command.source, command.destination, command.amount)
            record = self.journal.get(txid)
            if (record['source'], record['destination'], record['amount']) != (command.source, command.destination, command.amount):
                raise BankingError('TRANSACTION_ID_CONFLICT', 'Transaction ID belongs to a different transfer', 409)
            if newly_created:
                # Consistent A->B prepare order prevents opposite-direction
                # transfers acquiring shard locks in a cyclic order.
                legs = sorted(((source_owner, command.source, 'debit'),
                               (target_owner, command.destination, 'credit')))
                yes, reason = True, ''
                for index, (owner, account, role) in enumerate(legs):
                    yes, reason = await self._prepare(owner, txid, account, role, command.amount)
                    if not yes:
                        break
                    if index == 0:
                        self._inject_crash(txid, 'after-first-prepare')
                # Durable decision BEFORE sending COMMIT or ROLLBACK PREPARED.
                self.journal.decide(txid, 'COMMIT' if yes else 'ABORT', reason)
                LOGGER.info('coordinator.durable_decision txid=%s decision=%s reason=%s', txid, 'COMMIT' if yes else 'ABORT', reason)
                if yes:
                    self._inject_crash(txid, 'after-durable-commit')
            record = self.journal.get(txid)
            # Durable participant ABORT fences guarantee that an uncertain
            # in-flight PREPARE cannot succeed *after* ABORT acknowledgement.
            # If still preparing, participant responds PENDING; watchdog retries.
            if record['state'] in ('COMMIT', 'ABORT'):
                record = await self._deliver(txid)
            result = self.public_state(record)
            if result['status'] == 'COMMITTED':
                return 200, result
            if result['status'] == 'ABORTED':
                return 409, result
            return 202, result
