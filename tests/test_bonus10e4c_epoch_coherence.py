"""10E-4C1 API routing-epoch gate; no database or live cutover performed."""
from __future__ import annotations

import asyncio
import json
from pathlib import Path
from uuid import uuid4

import httpx
import pytest

from bank_platform.common.shard_map import load_shard_map
from bank_platform.common.validation import BankingError
from bank_platform.coordinator.app import create_app as coordinator_app
from bank_platform.coordinator.engine import CoordinatorEngine, DecisionJournal
from bank_platform.gateway.app import create_app as gateway_app
from bank_platform.migration.routing_guard import (assert_configuration, epoch_agrees,
                                                    probe_coordinator_epoch)

ROOT = Path(__file__).resolve().parents[1]
MAP3 = load_shard_map(ROOT / 'config/shard_map_3shard.example.json')
MAP2 = load_shard_map(ROOT / 'config/shard_map.json')


class CohesionUpstream:
    def __init__(self):
        self.coordinator_epoch = 2
        self.coordinator_healthy = True
        self.coordinator_network_error = False
        self.calls = []
        self.writes = []

    def __call__(self, req):
        self.calls.append((req.url.host, req.url.path))
        if req.url.host == 'coordinator':
            if self.coordinator_network_error:
                raise httpx.ConnectError('coordinator unreachable', request=req)
            if req.url.path == '/health':
                return httpx.Response(200, json={
                    'status': 'alive', 'service': 'coordinator',
                    'transactions_ready': self.coordinator_healthy,
                    'routing_epoch': self.coordinator_epoch})
            if req.url.path.startswith('/internal/transactions/'):
                return httpx.Response(404, json={'status': 'UNKNOWN'})
            if req.url.path == '/internal/transactions':
                data = json.loads(req.content)
                self.writes.append(('TPC', data))
                return httpx.Response(200, json={'status': 'COMMITTED', 'decision': 'COMMIT',
                                                 'transaction_id': data['transaction_id']})
        name = req.url.host.replace('bank_', '').upper()
        shard = name[0]
        if req.url.path == '/ready':
            return httpx.Response(200, json={'status': 'ready', 'shard': shard,
                                             'replica': name, 'storage_ready': True})
        if req.url.path.startswith('/internal/accounts/'):
            return httpx.Response(200, json={'account': req.url.path.rsplit('/', 1)[-1],
                                             'balance': 1000, 'shard': shard, 'replica': name})
        if req.url.path in ('/internal/operations', '/internal/transfers'):
            data = json.loads(req.content)
            self.writes.append((shard, data))
            return httpx.Response(200, json={'balance': 1000, 'shard': shard, 'replica': name})
        return httpx.Response(404)


async def gateway_session(upstream, job):
    app = gateway_app(MAP3, transport=httpx.MockTransport(upstream), poll_interval=3600)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                     base_url='http://gateway') as client:
            await job(client, upstream)


def configure(monkeypatch):
    monkeypatch.setenv('BANK_EPOCH_PROPAGATION', '1')
    monkeypatch.setenv('BANK_REQUIRE_ROUTING_EPOCH', '1')


def test_configuration_requires_epoch_propagation(monkeypatch, tmp_path):
    monkeypatch.delenv('BANK_EPOCH_PROPAGATION', raising=False)
    monkeypatch.setenv('BANK_REQUIRE_ROUTING_EPOCH', '1')
    with pytest.raises(ValueError, match='BANK_EPOCH_PROPAGATION'):
        assert_configuration()
    with pytest.raises(ValueError, match='BANK_EPOCH_PROPAGATION'):
        gateway_app(MAP3)
    with pytest.raises(ValueError, match='BANK_EPOCH_PROPAGATION'):
        coordinator_app(journal=DecisionJournal(str(tmp_path / 'j.db')))


@pytest.mark.parametrize('bad', [None, {}, {'routing_epoch': 2},
    {'status': 'alive', 'service': 'coordinator', 'transactions_ready': True},
    {'status': 'alive', 'service': 'coordinator', 'transactions_ready': True, 'routing_epoch': True},
    {'status': 'alive', 'service': 'coordinator', 'transactions_ready': True, 'routing_epoch': '2'},
    {'status': 'alive', 'service': 'coordinator', 'transactions_ready': False, 'routing_epoch': 2},
    {'status': 'alive', 'service': 'fake', 'transactions_ready': True, 'routing_epoch': 2},
    {'status': 'ready', 'service': 'coordinator', 'transactions_ready': True, 'routing_epoch': 2},
])
def test_identity_parser_fails_closed(bad):
    assert not epoch_agrees(bad, 2)


def test_identity_parser_accepts_exact_epoch_only():
    data = {'status': 'alive', 'service': 'coordinator',
            'transactions_ready': True, 'routing_epoch': 2}
    assert epoch_agrees(data, 2)
    assert not epoch_agrees(data, 1)
    assert not epoch_agrees(data, 3)


def test_gateway_old_or_new_map_mismatch_blocks_every_money_path(monkeypatch):
    configure(monkeypatch)
    upstream = CohesionUpstream()
    upstream.coordinator_epoch = 1
    async def scenario(client, upstream):
        ready = await client.get('/ready')
        assert ready.status_code == 503
        assert ready.json()['epoch_coherent'] is False
        assert ready.json()['routing_epoch'] == 2
        for method, url, payload in (
            ('GET', '/api/v1/accounts/A1700/balance', None),
            ('POST', '/api/v1/operations', {'action':'balance', 'account':'A1700'}),
            ('POST', '/api/v1/operations', {'action':'deposit', 'account':'A1700','amount':1}),
            ('POST', '/api/v1/operations', {'action':'transfer', 'from_account':'A1400',
                                            'to_account':'A1600', 'amount':1}),
        ):
            response = await client.request(method, url, json=payload)
            assert response.status_code == 503
            assert response.json()['error']['code'] == 'ROUTING_EPOCH_UNAVAILABLE'
        # Historical transaction lookups remain available despite map mismatch.
        historic = await client.get('/api/v1/transactions/' + str(uuid4()))
        assert historic.status_code == 404
        assert not upstream.writes
        assert not any(path.startswith('/internal/accounts/') for _, path in upstream.calls)
    asyncio.run(gateway_session(upstream, scenario))


def test_gateway_matching_epoch_forwards_self_selected_epoch(monkeypatch):
    configure(monkeypatch)
    upstream = CohesionUpstream()
    async def scenario(client, upstream):
        ready = await client.get('/ready')
        assert ready.status_code == 200 and ready.json()['epoch_coherent'] is True
        response = await client.get('/api/v1/accounts/A1600/balance')
        assert response.status_code == 200 and response.json()['shard'] == 'C'
        response = await client.post('/api/v1/operations', json={
            'action':'deposit', 'account':'A1600', 'amount':7})
        assert response.status_code == 200
        response = await client.post('/api/v1/operations', json={
            'action':'transfer', 'from_account':'A1400','to_account':'A1600',
            'amount':2})
        assert response.status_code == 200
        assert ('C', {'action':'deposit','account':'A1600','amount':7,'owner_epoch':2}) in upstream.writes
        assert any(shard == 'TPC' and data['owner_epoch'] == 2
                   for shard, data in upstream.writes)
        fail_public = await client.post('/api/v1/operations', json={
            'action':'deposit', 'account':'A1600', 'amount':7, 'owner_epoch':1})
        assert fail_public.status_code == 400
    asyncio.run(gateway_session(upstream, scenario))


@pytest.mark.parametrize('failure', ['offline', 'notready', 'wrong-epoch'])
def test_strict_gateway_coordinator_failures_block_writes(monkeypatch, failure):
    configure(monkeypatch)
    upstream = CohesionUpstream()
    if failure == 'offline': upstream.coordinator_network_error = True
    elif failure == 'notready': upstream.coordinator_healthy = False
    else: upstream.coordinator_epoch = 3
    async def scenario(client, upstream):
        response = await client.post('/api/v1/operations', json={
            'action':'withdraw', 'account':'A1600', 'amount':2})
        assert response.status_code == 503
        assert not upstream.writes
    asyncio.run(gateway_session(upstream, scenario))


def test_default_mode_remains_backward_compatible(monkeypatch):
    monkeypatch.delenv('BANK_REQUIRE_ROUTING_EPOCH', raising=False)
    monkeypatch.delenv('BANK_EPOCH_PROPAGATION', raising=False)
    upstream = CohesionUpstream()
    upstream.coordinator_epoch = 1  # Old coordinator, new gateway; default mode doesn't gate.
    async def scenario(client, upstream):
        response = await client.get('/ready')
        assert response.status_code == 200
        assert 'routing_epoch' not in response.json()
        response = await client.post('/api/v1/operations', json={
            'action': 'deposit','account':'A1600','amount':3})
        assert response.status_code == 200
        assert 'owner_epoch' not in upstream.writes[-1][1]
    asyncio.run(gateway_session(upstream, scenario))


class ParticipantUpstream:
    def __init__(self): self.prepared = []
    def __call__(self, req):
        host = req.url.host
        if req.url.path == '/ready':
            shard = host.replace('bank_', '').upper()[0]
            return httpx.Response(200, json={'status':'ready', 'shard': shard, 'storage_ready': True})
        payload = json.loads(req.content)
        if req.url.path == '/internal/tpc/prepare':
            self.prepared.append(payload)
            return httpx.Response(200, json={'status':'PREPARED'})
        if req.url.path == '/internal/tpc/decision':
            return httpx.Response(200, json={'status':'COMMITTED'})
        return httpx.Response(404)


async def coordinator_transfer(tmp_path, map_value, body, fake):
    journal = DecisionJournal(str(tmp_path / 'j.sqlite3'))
    journal.initialize()
    async with httpx.AsyncClient(transport=httpx.MockTransport(fake)) as client:
        engine = CoordinatorEngine(journal, map_value, client)
        return await engine.transfer(body)


def request(epoch=None):
    payload = {'action':'transfer','from_account':'A1400','to_account':'A1600',
               'amount':5, 'transaction_id':str(uuid4())}
    if epoch is not None: payload['owner_epoch'] = epoch
    return payload


@pytest.mark.parametrize('bad', [None, 1, 3, True, '2', 2.0])
def test_coordinator_strict_refuses_missing_stale_or_bad_epoch(monkeypatch,tmp_path,bad):
    configure(monkeypatch)
    fake = ParticipantUpstream()
    command = request(bad)
    with pytest.raises(BankingError) as exc:
        asyncio.run(coordinator_transfer(tmp_path, MAP3, command, fake))
    assert exc.value.code in ('ROUTING_EPOCH_MISMATCH','INVALID_OWNER_EPOCH')
    assert not fake.prepared
    j = DecisionJournal(str(tmp_path / 'j.sqlite3'))
    assert j.get(command['transaction_id']) is None


def test_coordinator_strict_correct_epoch_creates_durable_2pc(monkeypatch,tmp_path):
    configure(monkeypatch)
    fake = ParticipantUpstream()
    code, data = asyncio.run(coordinator_transfer(tmp_path, MAP3, request(2), fake))
    assert code == 200 and data['owner_epoch'] == 2
    assert len(fake.prepared) == 2
    assert all(x['owner_epoch'] == 2 for x in fake.prepared)


def test_coordinator_optional_claim_still_rejects_wrong_epoch(monkeypatch,tmp_path):
    monkeypatch.delenv('BANK_REQUIRE_ROUTING_EPOCH', raising=False)
    monkeypatch.setenv('BANK_EPOCH_PROPAGATION','1')
    fake = ParticipantUpstream()
    with pytest.raises(BankingError) as exc:
        asyncio.run(coordinator_transfer(tmp_path, MAP3, request(1), fake))
    assert exc.value.code == 'ROUTING_EPOCH_MISMATCH'


def test_coordinator_health_exposes_current_epoch(monkeypatch,tmp_path):
    monkeypatch.delenv('BANK_REQUIRE_ROUTING_EPOCH', raising=False)
    monkeypatch.delenv('BANK_EPOCH_PROPAGATION', raising=False)
    app = coordinator_app(journal=DecisionJournal(str(tmp_path / 'j.sqlite3')),
                          transport=httpx.MockTransport(ParticipantUpstream()))
    async def scenario():
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                         base_url='http://coordinator') as client:
                data = (await client.get('/health')).json()
                assert data['routing_epoch'] == 1
                assert data['transactions_ready'] is True
    asyncio.run(scenario())


def test_probe_fail_closed_on_missing_client_and_network():
    assert asyncio.run(probe_coordinator_epoch(None, 'http://coordinator', 2)) is False
    def upstream(req):
        raise httpx.ConnectError('down', request=req)
    async def case():
        async with httpx.AsyncClient(transport=httpx.MockTransport(upstream)) as client:
            assert not await probe_coordinator_epoch(client, 'http://coordinator', 2)
    asyncio.run(case())
