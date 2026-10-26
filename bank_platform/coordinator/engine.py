"""Coordinator 2PC state machine with fsynced SQLite decision journal.

Phase 5: synchronous prepare/decision/delivery. Phase 6 will add startup
reconciliation and deterministic crash injection for records left PENDING.
"""
from __future__ import annotations

import asyncio
import logging
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
                for owner, account, role in legs:
                    yes, reason = await self._prepare(owner, txid, account, role, command.amount)
                    if not yes:
                        break
                # Durable decision BEFORE sending COMMIT or ROLLBACK PREPARED.
                self.journal.decide(txid, 'COMMIT' if yes else 'ABORT', reason)
                LOGGER.info('coordinator.durable_decision txid=%s decision=%s reason=%s', txid, 'COMMIT' if yes else 'ABORT', reason)
            record = self.journal.get(txid)
            # Unknown PREPARE response is not a reliable NO vote: the
            # participant may still finish preparing after a network timeout.
            # Preserve ABORT durably but do NOT falsely acknowledge it.
            # Phase 6 reconciles these pending abort decisions.
            if (record['state'] in ('COMMIT', 'ABORT')
                    and not (record['state'] == 'ABORT' and record['reason'] == 'UNCERTAIN_PREPARE')):
                record = await self._deliver(txid)
            result = self.public_state(record)
            if result['status'] == 'COMMITTED':
                return 200, result
            if result['status'] == 'ABORTED':
                return 409, result
            return 202, result
