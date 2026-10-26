#!/usr/bin/env python3
"""Phase 6: crash recovery on VPS (Docker Compose, one Coordinator only).

Default: NON-DISRUPTIVE preflight. Run with --crash-tests to deliberately kill
ONLY the distributed-banking Coordinator three times. Successful execution is
balance-neutral. On failures, STOP and inspect; never rerun a failed transfer
with a fresh ID or delete database/coordinator volumes.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import time
import urllib.error
import urllib.request
from uuid import uuid4

BASE = 'http://127.0.0.1:8001'
SRC, DST = 'A0802', 'A1802'


def command(*parts):
    proc = subprocess.run(list(parts), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if proc.returncode:
        raise RuntimeError(f"command failed {parts}: {proc.stderr.strip()}")
    return proc.stdout.strip()


def request(path: str, data=None, timeout=28):
    encoded = json.dumps(data).encode() if data is not None else None
    req = urllib.request.Request(BASE + path, data=encoded, method='POST' if data is not None else 'GET',
                                 headers={'Content-Type': 'application/json'})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as res:
            return res.status, json.load(res)
    except urllib.error.HTTPError as exc:
        try:
            return exc.code, json.load(exc)
        except ValueError:
            return exc.code, {'error': 'Non-JSON HTTP response'}
    except (urllib.error.URLError, TimeoutError, ConnectionError, OSError) as exc:
        return None, {'error': type(exc).__name__}


def balance(account):
    code, data = request('/api/v1/accounts/' + account + '/balance')
    if code != 200 or data.get('account') != account:
        raise RuntimeError(f'Balance unavailable for {account}: {code} {data}')
    return data['balance']


def status(txid):
    return request('/api/v1/transactions/' + txid)


def transfer(src, dst, amount, txid):
    return request('/api/v1/operations', {'action': 'transfer', 'from_account': src,
                   'to_account': dst, 'amount': amount, 'transaction_id': txid})


def no_prepared():
    for shard in ('a', 'b'):
        result = command('docker', 'compose', 'exec', '-T', f'shard_{shard}_db',
                         'psql', '-U', 'bank', '-d', f'bank_{shard}', '-Atc',
                         'SELECT count(*) FROM pg_prepared_xacts WHERE database=current_database()')
        if result != '0':
            raise RuntimeError(f'Shard {shard} still has {result} prepared transactions; STOP')


def arm(txid, gate):
    # No public crash injection API exists. An operator must have Docker access
    # to write a one-time, exact-UUID marker in the coordinator named volume.
    program = (
        'from pathlib import Path; '
        'p=Path("/app/state/faults"); p.mkdir(exist_ok=True); '
        f'(p / "{txid}.{gate}").write_text("armed")'
    )
    command('docker', 'compose', 'exec', '-T', 'coordinator', 'python', '-c', program)


def coordinator_state():
    name = command('docker', 'compose', 'ps', '-a', '-q', 'coordinator')
    if not name:
        raise RuntimeError('Coordinator container not found')
    return command('docker', 'inspect', '--format', '{{.State.Status}}:{{.State.ExitCode}}', name)


def wait_final(txid, expected):
    deadline = time.monotonic() + 65
    while time.monotonic() < deadline:
        code, data = status(txid)
        if code == 200 and data.get('status') == expected:
            assert data.get('decision') == ('COMMIT' if expected == 'COMMITTED' else 'ABORT'), data
            assert set(data.get('acknowledged_shards', [])) == {'A', 'B'}, data
            return data
        if code == 200 and data.get('status') in ('COMMITTED', 'ABORTED'):
            raise RuntimeError(f'Wrong final status txid={txid}: {data}')
        time.sleep(1)
    raise RuntimeError(f'Recovery timeout txid={txid}: last={status(txid)}. STOP; no fresh ID retry.')


def assert_balances(expected):
    actual = (balance(SRC), balance(DST))
    if actual != expected:
        raise RuntimeError(f'Balance invariant FAILED actual={actual} expected={expected}; STOP')


def crash_case(gate, expected, amount, baseline):
    txid = str(uuid4())
    print(f'CASE {gate} txid={txid}', flush=True)
    arm(txid, gate)
    code, data = transfer(SRC, DST, amount, txid)
    if code == 200 or (code == 202 and data.get('status') == 'COMMITTED'):
        raise RuntimeError(f'Coordinator did not crash when armed, txid={txid}: {code} {data}')
    state = coordinator_state()
    if state != 'exited:86':
        raise RuntimeError(f'Expected one-shot coordinator crash (exited:86), found {state}, txid={txid}; STOP')
    command('docker', 'compose', 'start', 'coordinator')
    final = wait_final(txid, expected)
    no_prepared()
    if expected == 'ABORTED':
        assert_balances(baseline)
        print(f'PASS: undecided prepared transaction rolled back after crash; {final}', flush=True)
    else:
        assert_balances((baseline[0] - amount, baseline[1] + amount))
        # Exactly-once replay of the ORIGINAL transaction ID (safe).
        repeat_code, repeat = transfer(SRC, DST, amount, txid)
        if repeat_code != 200 or repeat.get('status') != 'COMMITTED':
            raise RuntimeError(f'Idempotent replay failed txid={txid}: {repeat_code} {repeat}')
        assert_balances((baseline[0] - amount, baseline[1] + amount))
        # Neutralize test balances with a NEW, distinct *reverse* transaction.
        reverse_id = str(uuid4())
        rev_code, rev = transfer(DST, SRC, amount, reverse_id)
        if rev_code != 200 or rev.get('status') != 'COMMITTED':
            raise RuntimeError(f'Reverse test transfer uncertain txid={reverse_id}: {rev_code} {rev}. STOP')
        assert_balances(baseline)
        no_prepared()
        print(f'PASS: durable COMMIT recovered after {gate}, exactly once, balances restored', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--crash-tests', action='store_true', help='DISRUPTIVE: kill only Coordinator 3 times')
    args = parser.parse_args()
    command('docker', 'compose', 'config', '--quiet')
    health, gateway = request('/health')
    if health != 200:
        raise RuntimeError(f'Gateway not healthy: {health} {gateway}')
    initial = (balance(SRC), balance(DST))
    print('Phase 6 initial balances:', dict(zip((SRC, DST), initial)))
    if initial[0] < 40 or initial[1] < 40:
        raise RuntimeError('Test accounts have insufficient balance')
    no_prepared()
    print('PASS: non-disruptive Phase 6 preflight; no prepared transactions')
    if not args.crash_tests:
        print('Crash tests NOT run. Explicitly add --crash-tests to inject coordinator crashes.')
        return
    # Tests deliberately kill only Coordinator, never PostgreSQL/Assignment 1.
    for gate, state in (
        ('after-first-prepare', 'ABORTED'),
        ('after-durable-commit', 'COMMITTED'),
        ('after-first-ack', 'COMMITTED'),
    ):
        crash_case(gate, state, 9, initial)
    assert_balances(initial)
    no_prepared()
    print('PASS: Phase 6 coordinator crash recovery, exactly-once and invariant checks completed')


if __name__ == '__main__':
    main()
