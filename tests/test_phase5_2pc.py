"""Phase 5: 2PC state machine, fsynced decisions, error paths and idempotency.

These fakes verify orchestration. The live script separately proves PostgreSQL
PREPARE TRANSACTION + COMMIT PREPARED on the VPS.
"""
import asyncio
import json
from pathlib import Path
from uuid import uuid4

import httpx
import pytest

from bank_platform.common.shard_map import load_shard_map
from bank_platform.common.validation import BankingError
from bank_platform.coordinator.engine import CoordinatorEngine, DecisionJournal
from bank_platform.gateway.app import create_app as gateway_app
from bank_platform.shard.twopc import normalize_txid, prepared_gid

MAP = load_shard_map(str(Path(__file__).resolve().parents[1] / 'config/shard_map.json'))


def body(amount=7, txid=None):
    return {'action': 'transfer', 'from_account': 'A0801', 'to_account': 'A1801',
            'amount': amount, 'transaction_id': txid or str(uuid4())}


class MockParticipants:
    def __init__(self):
        self.balance = {'A0801': 100, 'A1801': 200}
        self.pending = {}
        self.committed = set()
        self.aborted = set()
        self.prepares = []
        self.decisions = []
        self.reject_credit = False
        self.fail_second_commit = False
        self.uncertain_prepare = False
        self.dead = set()

    def __call__(self, req):
        shard = req.url.host.replace('bank_', '')[0].upper()
        if req.url.path == '/ready':
            if shard in self.dead:
                raise httpx.ConnectError('dead', request=req)
            return httpx.Response(200, json={'status': 'ready', 'shard': shard, 'storage_ready': True})
        payload = json.loads(req.content)
        key = (shard, payload['transaction_id'])
        if req.url.path == '/internal/tpc/prepare':
            self.prepares.append(key)
            if self.uncertain_prepare and shard == 'B':
                raise httpx.ReadTimeout('ambiguous prepare response', request=req)
            if key in self.committed:
                return httpx.Response(200, json={'status': 'COMMITTED'})
            if key in self.aborted:
                return httpx.Response(409, json={'error': 'TRANSACTION_ABORTED'})
            if key in self.pending:
                return httpx.Response(200, json={'status': 'PREPARED'})
            if shard == 'A' and self.balance['A0801'] < payload['amount']:
                return httpx.Response(409, json={'error': 'INSUFFICIENT_FUNDS'})
            if shard == 'B' and self.reject_credit:
                return httpx.Response(409, json={'error': 'BALANCE_OVERFLOW'})
            self.pending[key] = payload
            return httpx.Response(200, json={'status': 'PREPARED'})
        if req.url.path == '/internal/tpc/decision':
            decision = payload['decision']
            self.decisions.append((key, decision))
            if shard == 'B' and decision == 'COMMIT' and self.fail_second_commit:
                return httpx.Response(503, json={'error': 'DECISION_UNCERTAIN'})
            if key in self.pending:
                command = self.pending.pop(key)
                if decision == 'COMMIT':
                    account = 'A0801' if shard == 'A' else 'A1801'
                    self.balance[account] += -command['amount'] if shard == 'A' else command['amount']
                    self.committed.add(key)
            if decision == 'ABORT':
                self.aborted.add(key)  # fake durable fence
            return httpx.Response(200, json={'status': 'COMMITTED' if decision == 'COMMIT' else 'ABORTED'})
        return httpx.Response(404, json={'error': 'NOT_FOUND'})


async def drive(tmp_path, fake, command, existing_path=None):
    journal = DecisionJournal(str(existing_path or tmp_path / 'journal.sqlite3'))
    journal.initialize()
    async with httpx.AsyncClient(transport=httpx.MockTransport(fake)) as client:
        engine = CoordinatorEngine(journal, MAP, client)
        status, result = await engine.transfer(command)
    return journal, status, result


def test_cross_shard_commit_and_idempotent_replay(tmp_path):
    fake = MockParticipants()
    command = body()
    journal, status, result = asyncio.run(drive(tmp_path, fake, command))
    assert status == 200
    assert result['status'] == 'COMMITTED' and result['decision'] == 'COMMIT'
    assert set(result['acknowledged_shards']) == {'A', 'B'}
    assert fake.balance == {'A0801': 93, 'A1801': 207}
    _, again, result2 = asyncio.run(drive(tmp_path, fake, command))
    assert again == 200 and result2['transaction_id'] == command['transaction_id']
    assert fake.balance == {'A0801': 93, 'A1801': 207}
    assert len(fake.prepares) == 2  # no second prepare on replay
    assert journal.get(command['transaction_id'])['state'] == 'DONE'


def test_first_voter_rejects_no_money_moved(tmp_path):
    fake = MockParticipants()
    _, code, result = asyncio.run(drive(tmp_path, fake, body(amount=101)))
    assert code == 409 and result['status'] == 'ABORTED'
    assert result['reason'] == 'INSUFFICIENT_FUNDS'
    assert fake.balance == {'A0801': 100, 'A1801': 200}
    assert ('A', 'ABORT') in [(key[0], d) for key, d in fake.decisions]


def test_second_voter_rejects_prepared_debit_rolled_back(tmp_path):
    fake = MockParticipants()
    fake.reject_credit = True
    journal, code, result = asyncio.run(drive(tmp_path, fake, body()))
    assert code == 409 and result['reason'] == 'BALANCE_OVERFLOW'
    assert result['decision'] == 'ABORT' and journal.get(result['transaction_id'])['state'] == 'ABORTED'
    assert fake.pending == {} and fake.balance == {'A0801': 100, 'A1801': 200}


def test_durable_commit_incomplete_must_return_pending_then_redeliver(tmp_path):
    fake = MockParticipants()
    fake.fail_second_commit = True
    command = body()
    db_path = tmp_path / 'persistent.sqlite3'
    journal, code, result = asyncio.run(drive(tmp_path, fake, command, db_path))
    assert code == 202 and result['status'] == 'PENDING'
    assert result['decision'] == 'COMMIT' and result['acknowledged_shards'] == ['A']
    assert journal.get(command['transaction_id'])['state'] == 'COMMIT'
    assert fake.balance == {'A0801': 93, 'A1801': 200}
    fake.fail_second_commit = False
    _, code2, result2 = asyncio.run(drive(tmp_path, fake, command, db_path))
    assert code2 == 200 and result2['status'] == 'COMMITTED'
    assert fake.balance == {'A0801': 93, 'A1801': 207}
    assert journal.get(command['transaction_id'])['state'] == 'DONE'


def test_uncertain_prepare_can_be_aborted_after_durable_fencing(tmp_path):
    fake = MockParticipants()
    fake.uncertain_prepare = True
    command = body()
    journal, code, result = asyncio.run(drive(tmp_path, fake, command))
    # Mock ABORT creates the same durable late-prepare fence as PostgreSQL.
    assert code == 409 and result['status'] == 'ABORTED'
    assert result['decision'] == 'ABORT'
    assert result['reason'] == 'UNCERTAIN_PREPARE'
    assert set(result['acknowledged_shards']) == {'A', 'B'}
    assert journal.get(result['transaction_id'])['state'] == 'ABORTED'
    assert fake.pending == {}
    assert fake.balance == {'A0801': 100, 'A1801': 200}
    assert {('A', command['transaction_id']), ('B', command['transaction_id'])} <= fake.aborted


def test_reusing_transaction_id_for_different_transfer_is_rejected(tmp_path):
    fake = MockParticipants()
    command = body()
    asyncio.run(drive(tmp_path, fake, command))
    with pytest.raises(BankingError) as ex:
        asyncio.run(drive(tmp_path, fake, {**command, 'amount': 8}))
    assert ex.value.code == 'TRANSACTION_ID_CONFLICT'
    assert fake.balance == {'A0801': 93, 'A1801': 207}


def test_postgres_gids_are_fixed_and_safe():
    txid = str(uuid4())
    assert prepared_gid(txid, 'A') == 'bank_' + txid.replace('-', '') + '_a'
    assert normalize_txid(txid) == txid
    with pytest.raises(BankingError):
        normalize_txid('bad\'; DROP TABLE accounts; --')
    with pytest.raises(BankingError):
        normalize_txid('00000000-0000-0000-0000-000000000000')


def test_real_pg_prepared_transactions_enabled_and_ledger_is_atomic():
    base = Path(__file__).resolve().parents[1]
    compose = (base / 'compose.yaml').read_text()
    assert compose.count('max_prepared_transactions=100') == 2
    for shard in ('a', 'b'):
        sql = (base / 'database' / f'shard_{shard}.sql').read_text()
        assert 'CREATE TABLE IF NOT EXISTS tpc_participant_commits' in sql
        assert 'transaction_id UUID PRIMARY KEY' in sql
    module = (base / 'bank_platform/shard/twopc.py').read_text()
    for keyword in ("PREPARE TRANSACTION '", "COMMIT PREPARED '", "ROLLBACK PREPARED '"):
        assert keyword in module


def test_coordinator_api_unknown_status_and_nonready(tmp_path):
    from bank_platform.coordinator.app import create_app
    app = create_app(journal=DecisionJournal(str(tmp_path / 'coordinator.db')), transport=httpx.MockTransport(MockParticipants()))
    async def check():
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://coordinator') as client:
                res = await client.get('/internal/transactions/' + str(uuid4()))
                assert res.status_code == 404 and res.json()['status'] == 'UNKNOWN'
                c = body()
                res = await client.post('/internal/transactions', json=c)
                assert res.status_code == 200 and res.json()['status'] == 'COMMITTED'
                res = await client.get('/internal/transactions/' + c['transaction_id'])
                assert res.status_code == 200 and res.json()['decision'] == 'COMMIT'
    asyncio.run(check())


def test_gateway_cross_shard_success_and_status_query():
    txid = str(uuid4())
    def upstream(req):
        if req.url.path == '/ready':
            replica = req.url.host.replace('bank_', '').upper()
            return httpx.Response(200, json={'status':'ready','shard':replica[0],'replica':replica,'storage_ready':True})
        if req.url.path == '/internal/transactions' and req.url.host == 'coordinator':
            return httpx.Response(200, json={'transaction_id': txid, 'status': 'COMMITTED', 'decision': 'COMMIT', 'acknowledged_shards': ['A','B']})
        if req.url.path.startswith('/internal/transactions/'):
            return httpx.Response(200, json={'transaction_id':txid,'status':'COMMITTED','decision':'COMMIT'})
        return httpx.Response(404)
    app = gateway_app(transport=httpx.MockTransport(upstream), poll_interval=3600)
    async def check():
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://gateway') as client:
                res = await client.post('/api/v1/operations', json=body(txid=txid))
                assert res.status_code == 200 and res.json()['status'] == 'COMMITTED'
                res = await client.get('/api/v1/transactions/' + txid)
                assert res.status_code == 200 and res.json()['decision'] == 'COMMIT'
    asyncio.run(check())
