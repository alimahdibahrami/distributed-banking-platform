"""Source-only 10E groundwork: immutable participant routing, no live cutover."""
import asyncio
import json
import sqlite3
from pathlib import Path
from uuid import uuid4

import httpx
import pytest

from bank_platform.common.shard_map import load_shard_map
from bank_platform.coordinator.engine import CoordinatorEngine, DecisionJournal
from bank_platform.shard.twopc import prepared_gid

ROOT = Path(__file__).resolve().parents[1]
BASE = load_shard_map(ROOT / 'config/shard_map.json')
THREE = load_shard_map(ROOT / 'config/shard_map_3shard.example.json')


def test_three_shard_owner_and_legacy_remain_unchanged():
    assert BASE.version == 1
    assert BASE.owner_for('A1601').name == 'B'
    assert THREE.version == 2
    assert THREE.owner_for('A1000').name == 'A'
    assert THREE.owner_for('A1001').name == 'B'
    assert THREE.owner_for('A1500').name == 'B'
    assert THREE.owner_for('A1501').name == 'C'
    assert THREE.owner_for('A2000').name == 'C'
    assert prepared_gid(str(uuid4()), 'C').endswith('_c')


@pytest.mark.parametrize('mutator', [
    lambda d: d.update(version=1),
    lambda d: d['shards'][1].update(last='A1499'),
    lambda d: d['shards'][2].update(first='A1500'),
    lambda d: d['shards'][2].update(replicas=d['shards'][1]['replicas']),
    lambda d: d['shards'][2].update(id='B'),
])
def test_three_shard_map_rejects_inconsistent_cutover(tmp_path, mutator):
    data = json.loads((ROOT / 'config/shard_map_3shard.example.json').read_text())
    mutator(data)
    f = tmp_path / 'invalid.json'
    f.write_text(json.dumps(data))
    with pytest.raises(ValueError):
        load_shard_map(f)


def test_new_journal_pins_b_c_and_recovers(tmp_path):
    path = tmp_path / 'journal.db'
    j = DecisionJournal(str(path))
    j.initialize()
    tx = str(uuid4())
    legs = (('B', 'A1401', 'debit'), ('C', 'A1601', 'credit'))
    assert j.create(tx, 'A1401', 'A1601', 9, legs=legs, epoch=2)
    assert not j.create(tx, 'A1401', 'A1601', 9, legs=legs, epoch=3)
    assert j.get(tx)['owner_epoch'] == 2
    assert [(p['shard'], p['epoch']) for p in j.participants(tx)] == [('B', 2), ('C', 2)]
    j.decide(tx, 'COMMIT')
    j.acknowledge(tx, 'B')
    assert j.get(tx)['state'] == 'COMMIT'
    with pytest.raises(ValueError, match='nonparticipant'):
        j.acknowledge(tx, 'A')
    # Restart must not change the immutable historical participant set.
    resumed = DecisionJournal(str(path))
    resumed.initialize()
    assert resumed.participants(tx)[1]['shard'] == 'C'
    resumed.acknowledge(tx, 'C')
    assert resumed.get(tx)['state'] == 'DONE'
    assert {p['shard'] for p in resumed.participants(tx) if p['ack']} == {'B','C'}


def test_old_journal_schema_migrates_without_losing_decisions(tmp_path):
    path = tmp_path / 'old.db'
    con = sqlite3.connect(path)
    con.execute('''CREATE TABLE transactions (
      transaction_id TEXT PRIMARY KEY, source TEXT NOT NULL, destination TEXT NOT NULL,
      amount INTEGER NOT NULL, state TEXT NOT NULL,
      a_ack INTEGER NOT NULL DEFAULT 0, b_ack INTEGER NOT NULL DEFAULT 0,
      reason TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)''')
    tx = str(uuid4())
    con.execute("INSERT INTO transactions (transaction_id,source,destination,amount,state,a_ack) VALUES (?,?,?,?,?,1)",
                (tx, 'A0801', 'A1801', 5, 'COMMIT'))
    con.commit(); con.close()
    j = DecisionJournal(str(path)); j.initialize()
    assert j.get(tx)['owner_epoch'] == 1
    assert [(p['shard'], p['ack']) for p in j.participants(tx)] == [('A', 1), ('B', 0)]
    j.acknowledge(tx, 'B')
    assert j.get(tx)['state'] == 'DONE'


def test_bc_transfer_restarts_from_durable_commit(tmp_path):
    tx = str(uuid4())
    path = tmp_path / 'journal.db'
    j = DecisionJournal(str(path)); j.initialize()
    prepared = {}
    receipts = set()
    first_c_reply_lost = True
    async def run_transfer(recovery=False):
        nonlocal first_c_reply_lost
        def service(request):
            nonlocal first_c_reply_lost
            shard = request.url.host.split('_')[1].upper()[0]
            if request.url.path == '/ready':
                return httpx.Response(200, json={'shard':shard,'status':'ready','storage_ready':True})
            body = json.loads(request.content)
            key = (shard,body['transaction_id'])
            if request.url.path.endswith('/prepare'):
                prepared[key] = dict(body)
                return httpx.Response(200,json={'status':'PREPARED'})
            if body['decision'] == 'COMMIT' and shard == 'C' and first_c_reply_lost:
                first_c_reply_lost = False
                return httpx.Response(503,json={'error':'TEMPORARY_UNAVAILABLE'})
            if body['decision'] == 'COMMIT':
                receipts.add(key)
            return httpx.Response(200,json={'status':'COMMITTED' if body['decision']=='COMMIT' else 'ABORTED'})
        async with httpx.AsyncClient(transport=httpx.MockTransport(service)) as client:
            engine = CoordinatorEngine(DecisionJournal(str(path)), THREE, client)
            if recovery:
                await engine.recover_once(startup=True)
                return engine.public_state(j.get(tx))
            return await engine.transfer({'action':'transfer','from_account':'A1401','to_account':'A1601','amount':5,'transaction_id':tx})
    code, result = asyncio.run(run_transfer())
    assert code == 202 and result['decision'] == 'COMMIT'
    assert result['acknowledged_shards'] == ['B']
    assert [(p['shard'],p['epoch']) for p in j.participants(tx)] == [('B',2),('C',2)]
    result = asyncio.run(run_transfer(recovery=True))
    assert result['status'] == 'COMMITTED' and result['owner_epoch'] == 2
    assert len(receipts) == 2
    assert len(prepared) == 2


def test_c_storage_range_requires_matching_database_metadata(monkeypatch):
    from bank_platform.shard.storage import PostgresAccountStore, DbConfig
    monkeypatch.delenv('BANK_ACCOUNT_FIRST', raising=False)
    monkeypatch.delenv('BANK_ACCOUNT_LAST', raising=False)
    store = PostgresAccountStore('C', DbConfig('example','example','example','example'))
    assert store.owns('A1501') and store.owns('A2000')
    assert not store.owns('A1500')


def test_b_post_cutover_range_is_explicit_and_does_not_rewrite_data(monkeypatch):
    from bank_platform.shard.storage import PostgresAccountStore, DbConfig
    monkeypatch.setenv('BANK_ACCOUNT_FIRST', '1001')
    monkeypatch.setenv('BANK_ACCOUNT_LAST', '1500')
    store = PostgresAccountStore('B', DbConfig('example','example','example','example'))
    assert store.owns('A1500')
    assert not store.owns('A1501')
    assert (store.first, store.last) == (1001,1500)
