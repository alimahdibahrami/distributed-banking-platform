"""E2 contracts: public/internal epoch boundary and transaction-local SQL context.

These unit tests do not substitute for PostgreSQL trigger and 2PC fault tests.
"""
from __future__ import annotations

import asyncio
import json
from pathlib import Path
from uuid import uuid4

import httpx
import pytest

from bank_platform.common.validation import BankingError
from bank_platform.common.shard_map import load_shard_map
from bank_platform.gateway.app import create_app as gateway_app
from bank_platform.migration.epoch import enabled, outgoing_epoch, strip_internal_epoch, stale_epoch_error
from bank_platform.shard.app import create_app as bank_app
from bank_platform.shard.storage import DbConfig, PostgresAccountStore
from bank_platform.shard.twopc import Pg2pcParticipant
from bank_platform.coordinator.engine import CoordinatorEngine, DecisionJournal

BASE = Path(__file__).resolve().parents[1]
THREE = load_shard_map(BASE / 'config/shard_map_3shard.example.json')


class EpochFakeStore:
    def __init__(self):
        self.shard_id = 'C'
        self.records = {'A1601': 100, 'A1602': 200}
        self.calls = []

    def owns(self, account):
        return account in self.records

    def ready(self):
        return True

    def read_account(self, account):
        return self.records.get(account)

    def mutate(self, action, account, amount, *, owner_epoch=None):
        self.calls.append(('mutate', action, account, owner_epoch))
        self.records[account] += amount if action == 'deposit' else -amount
        return self.records[account]

    def transfer_local(self, source, destination, amount, *, owner_epoch=None):
        self.calls.append(('transfer', source, destination, owner_epoch))
        self.records[source] -= amount
        self.records[destination] += amount
        return self.records[source], self.records[destination]


async def send(app, method, url, **kw):
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
        return await client.request(method, url, **kw)


def test_epoch_feature_off_retains_legacy_strictness(monkeypatch):
    monkeypatch.delenv('BANK_EPOCH_PROPAGATION', raising=False)
    assert not enabled() and outgoing_epoch(2) == {}
    assert strip_internal_epoch({'account': 'A1601'}) == ({'account': 'A1601'}, None)
    with pytest.raises(BankingError, match='disabled'):
        strip_internal_epoch({'account': 'A1601', 'owner_epoch': 2})


@pytest.mark.parametrize('bad',[True, 0, -1, 1.0, '2', None])
def test_epoch_validation_rejects_unsafe_types(monkeypatch,bad):
    monkeypatch.setenv('BANK_EPOCH_PROPAGATION', '1')
    with pytest.raises(BankingError) as e:
        strip_internal_epoch({'owner_epoch': bad})
    assert e.value.code == 'INVALID_OWNER_EPOCH'


def test_epoch_helpers_forward_valid_integer_only(monkeypatch):
    monkeypatch.setenv('BANK_EPOCH_PROPAGATION', '1')
    assert outgoing_epoch(2) == {'owner_epoch': 2}
    assert strip_internal_epoch({'owner_epoch': 2, 'amount': 5}) == ({'amount': 5}, 2)
    for value in (True, 0, -1, 2.0):
        with pytest.raises(ValueError):
            outgoing_epoch(value)


def test_shard_api_receives_internal_epoch_without_exposing_it_as_validation_field(monkeypatch):
    monkeypatch.setenv('BANK_EPOCH_PROPAGATION', '1')
    monkeypatch.setenv('BANK_ENABLE_SHARD_C', '1')
    store = EpochFakeStore()
    app = bank_app('C','C1',store)
    async def run():
        result = await send(app, 'POST','/internal/operations',json={
            'action':'deposit', 'account':'A1601','amount':7,'owner_epoch':2})
        assert result.status_code == 200 and result.json()['balance'] == 107
        result = await send(app,'POST','/internal/transfers',json={
            'action':'transfer','from_account':'A1601','to_account':'A1602','amount':3,'owner_epoch':2})
        assert result.status_code == 200
        result = await send(app,'POST','/internal/operations',json={
            'action':'deposit','account':'A1601','amount':1,'owner_epoch':1.0})
        assert result.status_code == 400
    asyncio.run(run())
    assert store.calls == [('mutate','deposit','A1601',2),('transfer','A1601','A1602',2)]


def test_public_gateway_rejects_caller_supplied_epoch_but_forwards_its_own(monkeypatch):
    monkeypatch.setenv('BANK_EPOCH_PROPAGATION', '1')
    writes = []
    def upstream(req):
        host = req.url.host
        if req.url.path == '/ready':
            return httpx.Response(200,json={'status':'ready', 'shard':host.split('_')[1][0].upper(),
                'replica':host.split('_')[1].upper(), 'storage_ready':True})
        if req.url.path == '/internal/operations':
            data = json.loads(req.content)
            writes.append(data)
            return httpx.Response(200,json={'shard':'C','replica':host.split('_')[1].upper(),'balance':100})
        if host == 'coordinator' and req.url.path == '/health':
            return httpx.Response(200,json={'status':'alive','service':'coordinator','transactions_ready':True})
        return httpx.Response(404)
    app = gateway_app(THREE,transport=httpx.MockTransport(upstream),poll_interval=3600)
    async def run():
        async with app.router.lifespan_context(app):
            good = await send(app,'POST','/api/v1/operations',json={
                'action':'deposit','account':'A1601','amount':3})
            assert good.status_code == 200
            bad = await send(app,'POST','/api/v1/operations',json={
                'action':'deposit','account':'A1601','amount':3,'owner_epoch':999})
            assert bad.status_code == 400
    asyncio.run(run())
    assert writes == [{'action':'deposit','account':'A1601','amount':3,'owner_epoch':2}]


def test_coordinator_prepares_with_immutable_journal_epoch(monkeypatch,tmp_path):
    monkeypatch.setenv('BANK_EPOCH_PROPAGATION','1')
    seen=[]
    def upstream(req):
        host=req.url.host
        if req.url.path == '/ready':
            shard=host.split('_')[1][0].upper()
            return httpx.Response(200,json={'status':'ready','shard':shard,'storage_ready':True})
        body=json.loads(req.content)
        if req.url.path == '/internal/tpc/prepare':
            seen.append(body)
            return httpx.Response(200,json={'status':'PREPARED'})
        if req.url.path == '/internal/tpc/decision':
            return httpx.Response(200,json={'status':'COMMITTED'})
        return httpx.Response(404)
    async def run():
        j=DecisionJournal(str(tmp_path/'j.db'));j.initialize()
        async with httpx.AsyncClient(transport=httpx.MockTransport(upstream)) as client:
            e=CoordinatorEngine(j,THREE,client)
            status, result=await e.transfer({'action':'transfer','from_account':'A1401',
                    'to_account':'A1601','amount':4,'transaction_id':str(uuid4())})
            assert status==200 and result['owner_epoch']==2
    asyncio.run(run())
    assert len(seen)==2 and all(command['owner_epoch']==2 for command in seen)


class PgCursor:
    def __init__(self, calls):
        self.calls=calls
        self.current=None
    def __enter__(self):return self
    def __exit__(self,*args):return None
    def execute(self, query, params=None):
        self.calls.append((query,params))
        if 'FROM shard_metadata' in query:
            self.current=('C',1501,2000,1)
        elif 'SELECT balance' in query:
            self.current=(100,)
        elif 'pg_prepared_xacts' in query or 'tpc_participant_commits' in query or 'tpc_participant_aborts' in query:
            self.current=None
        elif 'pg_try_advisory_xact_lock' in query:
            self.current=(True,)
    def fetchone(self):return self.current


class PgConnection:
    def __init__(self,calls):self.calls=calls
    def __enter__(self):return self
    def __exit__(self,*args):return None
    def cursor(self):return PgCursor(self.calls)


def test_local_write_sets_epoch_before_row_lock_in_same_transaction(monkeypatch):
    monkeypatch.delenv('BANK_ACCOUNT_FIRST',raising=False)
    monkeypatch.delenv('BANK_ACCOUNT_LAST',raising=False)
    store=PostgresAccountStore('C',DbConfig('host','db','u','pw'))
    statements=[]
    store._connect=lambda:PgConnection(statements)
    assert store.mutate('deposit','A1601',8,owner_epoch=2)==108
    queries=[q for q,_ in statements]
    assert queries.index("SELECT set_config('bank.owner_epoch', %s, true)") < next(
        i for i,q in enumerate(queries) if 'FOR UPDATE' in q)
    assert ("SELECT set_config('bank.owner_shard', %s, true)",('C',)) in statements
    assert ("SELECT set_config('bank.owner_epoch', %s, true)",('2',)) in statements


def test_2pc_sets_epoch_before_prepared_write_in_same_transaction(monkeypatch):
    monkeypatch.delenv('BANK_ACCOUNT_FIRST',raising=False)
    monkeypatch.delenv('BANK_ACCOUNT_LAST',raising=False)
    store=PostgresAccountStore('C',DbConfig('host','db','u','pw'))
    statements=[]
    store._connect=lambda:PgConnection(statements)
    tpc=Pg2pcParticipant(store)
    result=tpc.prepare(transaction_id=str(uuid4()),account='A1601',role='credit',amount=7,owner_epoch=2)
    assert result=='PREPARED'
    queries=[q for q,_ in statements]
    assert queries.index("SELECT set_config('bank.owner_epoch', %s, true)") < next(
        i for i,q in enumerate(queries) if q.startswith('UPDATE accounts'))
    assert any(q.startswith('PREPARE TRANSACTION') for q in queries)


def test_explicit_database_fence_error_maps_to_409_only():
    class DbError(Exception):
        sqlstate='P0001'
    result=stale_epoch_error(DbError('STALE_OWNER_EPOCH: account=A1601'))
    assert result is not None and result.code=='STALE_OWNER_EPOCH' and result.http_status==409
    assert stale_epoch_error(DbError('another constraint')) is None
    assert stale_epoch_error(RuntimeError('STALE_OWNER_EPOCH')) is None


def test_migration_sql_has_transactional_trigger_cdc_and_opt_in_controls():
    sql=(BASE/'database/bonus10e2_migration_capture.sql').read_text()
    assert sql.startswith('-- Bonus 10E-2') and sql.rstrip().endswith('COMMIT;')
    for snippet in ('fence_enabled BOOLEAN NOT NULL DEFAULT FALSE',
                    'capture_enabled BOOLEAN NOT NULL DEFAULT FALSE',
                    'FROM bank_migration_fence WHERE singleton = 1 FOR SHARE',
                    "current_setting('bank.owner_epoch', true)",
                    'BEFORE INSERT OR UPDATE OR DELETE ON accounts',
                    'AFTER UPDATE ON accounts',
                    'UNIQUE (account_id, account_revision)',
                    'NEW.migration_revision := OLD.migration_revision + 1'):
        assert snippet in sql
