"""Phase 6: coordinator restart, journal recovery and crash-fault control."""
import asyncio
import json
from pathlib import Path
from uuid import uuid4

import httpx
import pytest

from bank_platform.coordinator.engine import DecisionJournal, CoordinatorEngine
from bank_platform.coordinator.app import create_app
from bank_platform.shard.twopc import advisory_key
from tests.test_phase5_2pc import MAP, MockParticipants, body, drive


def run(coro):
    return asyncio.run(coro)


def test_startup_replays_durable_commit_after_partial_ack(tmp_path):
    fake = MockParticipants()
    fake.fail_second_commit = True
    command = body()
    path = tmp_path / 'journal.db'
    journal, code, response = run(drive(tmp_path, fake, command, path))
    assert code == 202 and response['acknowledged_shards'] == ['A']
    assert response['decision'] == 'COMMIT' and fake.balance == {'A0801': 93, 'A1801': 200}
    fake.fail_second_commit = False

    async def recover():
        async with httpx.AsyncClient(transport=httpx.MockTransport(fake)) as client:
            # New process, new engine, same persistent journal.
            engine = CoordinatorEngine(DecisionJournal(str(path)), MAP, client)
            await engine.recover_once(startup=True)

    run(recover())
    assert journal.get(command['transaction_id'])['state'] == 'DONE'
    assert fake.balance == {'A0801': 93, 'A1801': 207}
    run(recover())
    assert fake.balance == {'A0801': 93, 'A1801': 207}  # exactly once


def test_undecided_new_recovers_as_aborted_and_fences_late_prepare(tmp_path):
    fake = MockParticipants()
    path = tmp_path / 'journal.db'
    journal = DecisionJournal(str(path))
    journal.initialize()
    command = body()
    txid = command['transaction_id']
    journal.create(txid, 'A0801', 'A1801', 7)
    # Crash after A's PREPARE was durable, before coordinator decision.
    fake.pending[('A', txid)] = {'amount': 7}

    async def recover():
        async with httpx.AsyncClient(transport=httpx.MockTransport(fake)) as client:
            engine = CoordinatorEngine(DecisionJournal(str(path)), MAP, client)
            await engine.recover_once(startup=True)
    run(recover())
    record = journal.get(txid)
    assert record['state'] == 'ABORTED' and record['reason'] == 'RECOVERED_UNDECIDED'
    assert fake.pending == {}
    assert fake.balance == {'A0801': 100, 'A1801': 200}
    # A delayed attempt to prepare cannot resurrect an aborted tx.
    async def late():
        async with httpx.AsyncClient(transport=httpx.MockTransport(fake)) as client:
            return await client.post('http://bank_b1:8000/internal/tpc/prepare', json={
                'transaction_id': txid, 'account': 'A1801', 'role': 'credit', 'amount': 7,
            })
    assert run(late()).status_code == 409
    run(recover())
    assert journal.get(txid)['state'] == 'ABORTED'


def test_incomplete_abort_replays_without_unsafe_commit(tmp_path):
    fake = MockParticipants()
    fake.dead.add('B')
    path = tmp_path / 'journal.db'
    journal, code, result = run(drive(tmp_path, fake, body(), path))
    assert code == 202 and result['decision'] == 'ABORT'
    fake.dead.clear()
    async def recover():
        async with httpx.AsyncClient(transport=httpx.MockTransport(fake)) as client:
            engine = CoordinatorEngine(journal, MAP, client)
            await engine.recover_once(startup=True)
    run(recover())
    assert journal.get(result['transaction_id'])['state'] == 'ABORTED'
    assert fake.balance == {'A0801': 100, 'A1801': 200}


def test_watchdog_processes_committed_decisions(tmp_path):
    fake = MockParticipants()
    fake.fail_second_commit = True
    command = body()
    journal, _, _ = run(drive(tmp_path, fake, command))
    fake.fail_second_commit = False
    async def recover():
        async with httpx.AsyncClient(transport=httpx.MockTransport(fake)) as client:
            engine = CoordinatorEngine(journal, MAP, client)
            await engine.recover_once(startup=False)
    run(recover())
    assert journal.get(command['transaction_id'])['state'] == 'DONE'


def test_fault_marker_is_exact_one_shot_and_local_only(tmp_path, monkeypatch):
    journal = DecisionJournal(str(tmp_path / 'coordinator.sqlite3'))
    journal.initialize()
    txid = str(uuid4())
    directory = tmp_path / 'faults'
    directory.mkdir()
    marker = directory / f'{txid}.after-durable-commit'
    marker.write_text('armed')
    engine = CoordinatorEngine(journal, MAP, None)
    exits = []

    def fake_exit(code):
        exits.append(code)
        raise RuntimeError('fault injected')

    monkeypatch.setattr('bank_platform.coordinator.engine.os._exit', fake_exit)
    with pytest.raises(RuntimeError, match='fault injected'):
        engine._inject_crash(txid, 'after-durable-commit')
    assert exits == [86] and not marker.exists()
    engine._inject_crash(txid, 'after-durable-commit')
    assert exits == [86]


def test_coordinator_lifespan_automatically_recovers_pending_commit(tmp_path):
    fake = MockParticipants()
    fake.fail_second_commit = True
    command = body()
    path = tmp_path / 'journal.sqlite3'
    journal, code, result = run(drive(tmp_path, fake, command, path))
    assert code == 202 and result['status'] == 'PENDING'
    fake.fail_second_commit = False
    app = create_app(journal=DecisionJournal(str(path)), transport=httpx.MockTransport(fake))

    async def check():
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://coordinator') as client:
                res = await client.get('/internal/transactions/' + command['transaction_id'])
                assert res.status_code == 200
                assert res.json()['status'] == 'COMMITTED'
    run(check())
    assert fake.balance == {'A0801': 93, 'A1801': 207}


def test_schema_fences_exist_on_both_shards():
    root = Path(__file__).resolve().parents[1]
    for shard in ('a', 'b'):
        text = (root / 'database' / f'shard_{shard}.sql').read_text()
        assert 'CREATE TABLE IF NOT EXISTS tpc_participant_aborts' in text
        assert 'transaction_id UUID PRIMARY KEY' in text
    assert advisory_key(str(uuid4())) != advisory_key(str(uuid4()))
