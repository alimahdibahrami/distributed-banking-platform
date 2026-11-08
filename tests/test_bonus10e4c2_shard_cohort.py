"""Shard cohort and read-fence contracts. No live database is modified here."""
from __future__ import annotations

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
from bank_platform.migration.shard_cohort import (
    assert_cohort_configuration, cohort_required, configured_epoch, replica_proves_epoch,
)
from bank_platform.shard.app import create_app as shard_app
from bank_platform.shard.storage import DbConfig, PostgresAccountStore

ROOT = Path(__file__).resolve().parents[1]
MAP3 = load_shard_map(ROOT / 'config/shard_map_3shard.example.json')


def configure(monkeypatch, *, epoch='2'):
    monkeypatch.setenv('BANK_REQUIRE_SHARD_COHORT', '1')
    monkeypatch.setenv('BANK_REQUIRE_ROUTING_EPOCH', '1')
    monkeypatch.setenv('BANK_EPOCH_PROPAGATION', '1')
    monkeypatch.setenv('BANK_ROUTING_EPOCH', epoch)


@pytest.mark.parametrize('raw', ['', '0', '02', '2.0', '-1', '+2', 'true', '۳', '3', '999'])
def test_reject_invalid_explicit_epoch(monkeypatch, raw):
    monkeypatch.setenv('BANK_ROUTING_EPOCH', raw)
    with pytest.raises(ValueError, match='BANK_ROUTING_EPOCH|assignment'):
        configured_epoch()


def test_cohort_requires_both_other_features(monkeypatch):
    monkeypatch.setenv('BANK_REQUIRE_SHARD_COHORT', '1')
    monkeypatch.delenv('BANK_REQUIRE_ROUTING_EPOCH', raising=False)
    monkeypatch.setenv('BANK_EPOCH_PROPAGATION', '1')
    with pytest.raises(ValueError, match='requires routing epoch'):
        assert_cohort_configuration()
    monkeypatch.setenv('BANK_REQUIRE_ROUTING_EPOCH', '1')
    monkeypatch.delenv('BANK_EPOCH_PROPAGATION', raising=False)
    with pytest.raises(ValueError, match='requires routing epoch'):
        assert_cohort_configuration()


@pytest.mark.parametrize('field,bad', [
    ('status', 'alive'), ('shard', 'B'), ('replica', 'C2'),
    ('storage_ready', False), ('banking_ready', False),
    ('ownership_verified', False), ('routing_epoch', 1),
    ('routing_epoch', True), ('routing_epoch', '2'),
])
def test_replica_proof_rejects_mismatched_fields(field, bad):
    data = {'status': 'ready', 'shard': 'C', 'replica': 'C1',
            'storage_ready': True, 'banking_ready': True,
            'ownership_verified': True, 'routing_epoch': 2}
    assert replica_proves_epoch(data, 'C', 'C1', 2)
    data[field] = bad
    assert not replica_proves_epoch(data, 'C', 'C1', 2)


class ProbeUpstream:
    def __init__(self):
        self.proof_for = {'A': 2, 'B': 2, 'C': 2}
        self.ready_for = {'A': True, 'B': True, 'C': True}
        self.calls = []

    def __call__(self, req):
        self.calls.append((req.method, req.url.host, req.url.path, str(req.url.query)))
        if req.url.host == 'coordinator' and req.url.path == '/health':
            return httpx.Response(200, json={
                'status': 'alive', 'service': 'coordinator',
                'transactions_ready': True, 'routing_epoch': 2})
        if req.url.host == 'coordinator':
            return httpx.Response(404)
        name = req.url.host.replace('bank_', '').upper()
        shard = name[0]
        if req.url.path == '/ready':
            return httpx.Response(200, json={
                'status': 'ready', 'shard': shard, 'replica': name,
                'storage_ready': True, 'banking_ready': True,
                'routing_epoch': self.proof_for[shard],
                'ownership_verified': self.ready_for[shard]})
        if req.url.path.startswith('/internal/accounts/'):
            return httpx.Response(200, json={
                'account': req.url.path.rsplit('/', 1)[-1],
                'shard': shard, 'replica': name, 'balance': 345})
        if req.url.path in ('/internal/operations', '/internal/transfers'):
            return httpx.Response(200, json={
                'shard': shard, 'replica': name, 'balance': 350})
        return httpx.Response(404)


async def scenario(upstream, fn):
    app = gateway_app(MAP3, transport=httpx.MockTransport(upstream), poll_interval=3600)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                    base_url='http://gateway') as client:
            await fn(client)


def test_gateway_strict_cohort_rejects_unproven_c_and_never_writes(monkeypatch):
    configure(monkeypatch)
    upstream = ProbeUpstream()
    upstream.ready_for['C'] = False
    async def check(client):
        assert (await client.get('/ready')).status_code == 503
        assert (await client.get('/api/v1/accounts/A1700/balance')).status_code == 503
        assert (await client.post('/api/v1/operations', json={
            'action': 'deposit', 'account': 'A1700', 'amount': 1})).status_code == 503
    asyncio.run(scenario(upstream, check))
    assert not any(method == 'POST' for method, *_ in upstream.calls)


def test_gateway_strict_cohort_forwards_epoch_on_balance_read(monkeypatch):
    configure(monkeypatch)
    upstream = ProbeUpstream()
    async def check(client):
        assert (await client.get('/ready')).status_code == 200
        response = await client.get('/api/v1/accounts/A1700/balance')
        assert response.status_code == 200 and response.json()['shard'] == 'C'
    asyncio.run(scenario(upstream, check))
    assert any(path == '/internal/accounts/A1700' and 'owner_epoch=2' in query
               for method, host, path, query in upstream.calls)


class StubCursor:
    def __init__(self, gate):
        self.gate = gate
        self.calls = []
        self.result = None

    def execute(self, query, params=None):
        self.calls.append((query, params))
        if 'FROM bank_migration_fence' in query:
            self.result = self.gate
        elif 'SELECT balance FROM accounts' in query:
            self.result = (1000,)
        else:
            self.result = None

    def fetchone(self): return self.result


class StubConnection:
    def __init__(self, cursor): self.c = cursor
    def __enter__(self): return self
    def __exit__(self, *args): return None
    def cursor(self): return self
    def execute(self, *args): return self.c.execute(*args)
    def fetchone(self): return self.c.fetchone()


def mock_store(monkeypatch, shard, gate, *, epoch='2', override=None):
    configure(monkeypatch, epoch=epoch)
    if override:
        for name, value in override.items(): monkeypatch.setenv(name, value)
    store = PostgresAccountStore(shard, DbConfig('db','name','user','pw'))
    fake = StubCursor(gate)
    monkeypatch.setattr(store, '_connect', lambda: StubConnection(fake))
    monkeypatch.setattr(store, '_verify_identity', lambda cursor: None)
    return store, fake


def test_fenced_old_b_read_rejected_even_if_balance_still_present(monkeypatch):
    store, cursor = mock_store(monkeypatch, 'B', ('C', 2, True), epoch='1')
    with pytest.raises(BankingError) as exc:
        store.read_account('A1700', owner_epoch=1)
    assert exc.value.code == 'STALE_OWNER_EPOCH'
    assert not any('SELECT balance FROM accounts' in sql for sql, _ in cursor.calls)


def test_source_b_before_cutover_serves_owned_moving_account(monkeypatch):
    store, cursor = mock_store(monkeypatch, 'B', ('B', 1, False), epoch='1')
    assert store.read_account('A1700', owner_epoch=1) == 1000
    assert any('FOR SHARE' in sql for sql, _ in cursor.calls)


def test_c_standby_cannot_read_even_with_correct_epoch(monkeypatch):
    store, cursor = mock_store(monkeypatch, 'C', ('B', 1, False))
    with pytest.raises(BankingError, match='fence'):
        store.read_account('A1700', owner_epoch=2)


def test_c_activated_reads_with_fenced_epoch(monkeypatch):
    store, _ = mock_store(monkeypatch, 'C', ('C', 2, True))
    assert store.read_account('A1700', owner_epoch=2) == 1000


def test_strict_reads_require_explicit_epoch(monkeypatch):
    store, cursor = mock_store(monkeypatch, 'C', ('C', 2, True))
    for wrong in (None, 1, True, '2'):
        with pytest.raises(BankingError, match='epoch'):
            store.read_account('A1700', owner_epoch=wrong)
    assert not cursor.calls


@pytest.mark.parametrize('shard,gate,epoch,expected', [
    ('B', ('B', 1, False), '1', True),
    ('B', ('C', 2, True), '2', True),
    ('B', ('C', 2, True), '1', False),
    ('C', ('B', 1, False), '2', False),
    ('C', ('C', 2, True), '2', True),
    ('C', ('C', 2, False), '2', False),
])
def test_replica_db_ownership_proof(monkeypatch, shard, gate, epoch, expected):
    store, _ = mock_store(monkeypatch, shard, gate, epoch=epoch)
    # A B instance from the previous epoch is necessarily rejected by its
    # own configuration when it claims a different routing epoch.
    if shard == 'B' and epoch == '1' and gate[0] == 'C':
        assert store.route_proof(1) is False
    else:
        assert store.route_proof(int(epoch)) is expected


def test_shard_app_proof_and_missing_read_epoch(monkeypatch):
    configure(monkeypatch)
    monkeypatch.setenv('BANK_ENABLE_SHARD_C', '1')
    class StubStore:
        shard_id='C'
        def ready(self): return True
        def route_proof(self, epoch): return epoch == 2
        def owns(self, account): return True
        def read_account(self, account, *, owner_epoch=None):
            if owner_epoch != 2: raise BankingError('STALE_OWNER_EPOCH', 'bad', 409)
            return 200
    app = shard_app('C', 'C1', StubStore())
    async def go():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                     base_url='http://local') as client:
            status = await client.get('/ready')
            assert status.status_code == 200
            assert status.json()['ownership_verified'] is True
            assert status.json()['routing_epoch'] == 2
            absent = await client.get('/internal/accounts/A1700')
            assert absent.status_code == 409
            owned = await client.get('/internal/accounts/A1700?owner_epoch=2')
            assert owned.status_code == 200
    asyncio.run(go())


def test_coordinator_rejects_unproven_participant_before_prepare(monkeypatch,tmp_path):
    configure(monkeypatch)
    fake=ProbeUpstream()
    fake.proof_for['C']=1
    journal = DecisionJournal(str(tmp_path / 'coordinator.sqlite3'))
    journal.initialize()
    async def go():
        async with httpx.AsyncClient(transport=httpx.MockTransport(fake)) as client:
            engine = CoordinatorEngine(journal, MAP3, client)
            answer = await engine._participant('C', 'POST','/internal/tpc/prepare', {
                'transaction_id': str(uuid4()), 'account': 'A1700',
                'role': 'credit', 'amount': 1, 'owner_epoch': 2})
            assert answer is None
    asyncio.run(go())
    assert not any(method == 'POST' for method, *_ in fake.calls)


def test_sql_guard_binds_to_physical_shard_not_client_epoch():
    ddl = (ROOT / 'database/bonus10e4c2_physical_owner_guard.sql').read_text()
    assert "current_owner <> physical_shard" in ddl
    assert "physical_shard = 'C' AND (NOT enforced OR current_epoch <> 2)" in ddl
    assert "FOR SHARE" in ddl
    assert "CREATE OR REPLACE FUNCTION bank_migration_write_guard()" in ddl
    assert 'DELETE FROM accounts' not in ddl
    assert 'UPDATE shard_metadata' not in ddl
