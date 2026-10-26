#!/usr/bin/env python3
"""Phase 5 real PostgreSQL 2PC acceptance. Runs via Gateway on localhost.

Uses only A0801/A1801. Successful run is balance neutral, including a
reversed compensating test transfer. On any uncertain outcome it STOPS: query
transaction_id and inspect pg_prepared_xacts; never blindly resubmit with a
new transaction ID.
"""
from __future__ import annotations

import json
import subprocess
import urllib.error
import urllib.request
from uuid import uuid4

BASE = 'http://127.0.0.1:8001'
SRC, DST = 'A0801', 'A1801'


def request(path, payload=None):
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(BASE + path, data=data, headers={'Content-Type': 'application/json'},
                                 method='POST' if data is not None else 'GET')
    try:
        with urllib.request.urlopen(req, timeout=35) as res:
            return res.status, json.load(res)
    except urllib.error.HTTPError as exc:
        return exc.code, json.load(exc)


def balance(account):
    code, data = request(f'/api/v1/accounts/{account}/balance')
    assert code == 200 and data.get('account') == account, (code, data)
    return data['balance']


def transfer(source, target, amount, txid=None):
    txid = txid or str(uuid4())
    code, result = request('/api/v1/operations', {
        'action': 'transfer', 'from_account': source, 'to_account': target,
        'amount': amount, 'transaction_id': txid,
    })
    assert code == 200 and result.get('status') == 'COMMITTED', (
        f'2PC txid={txid} did not finish: HTTP {code} {result}. STOP; check status before doing any other transfers')
    assert result['transaction_id'] == txid, result
    query_code, status = request(f'/api/v1/transactions/{txid}')
    assert query_code == 200 and status['status'] == 'COMMITTED' and status['decision'] == 'COMMIT', status
    assert set(status['acknowledged_shards']) == {'A', 'B'}, status
    return txid


def check_prepared():
    for shard, name in (('a', 'bank_a'), ('b', 'bank_b')):
        proc = subprocess.run(['docker', 'compose', 'exec', '-T', f'shard_{shard}_db',
                               'psql', '-U', 'bank', '-d', name, '-Atc',
                               'SELECT count(*) FROM pg_prepared_xacts WHERE database=current_database()'],
                              text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        assert proc.returncode == 0, proc.stderr
        assert proc.stdout.strip() == '0', f'Shard {shard.upper()} has unresolved prepared transactions: {proc.stdout}'
    print('PASS: zero left-over prepared PostgreSQL transactions')


def check():
    init = (balance(SRC), balance(DST))
    print('Initial balances:', dict(zip((SRC, DST), init)))
    assert init[0] > 20 and init[1] > 20, 'Fixture too small for test'

    # First vote NO: no money may move, decision must be durable ABORT.
    rejected_id = str(uuid4())
    code, result = request('/api/v1/operations', {
        'action': 'transfer', 'from_account': SRC, 'to_account': DST,
        'amount': init[0] + 1, 'transaction_id': rejected_id,
    })
    assert code == 409 and result.get('error', {}).get('code') == 'INSUFFICIENT_FUNDS', (code, result)
    code, status = request(f'/api/v1/transactions/{rejected_id}')
    assert code == 200 and status['status'] == 'ABORTED' and status['decision'] == 'ABORT', status
    assert (balance(SRC), balance(DST)) == init, 'Abort unexpectedly changed balances'
    print('PASS: durable ABORT on negative debit vote, balances unchanged')

    txid = transfer(SRC, DST, 17)
    assert (balance(SRC), balance(DST)) == (init[0] - 17, init[1] + 17), 'Commit did not update BOTH shards'
    print('PASS: real cross-shard commit changed exactly two balances; durable DONE')

    transfer(SRC, DST, 17, txid=txid)  # Identical request ID must not apply twice.
    assert (balance(SRC), balance(DST)) == (init[0] - 17, init[1] + 17), 'Duplicate transaction moved money twice'
    print('PASS: idempotent replay of committed cross-shard transfer')

    code, bad = request('/api/v1/operations', {
        'action': 'transfer', 'from_account': SRC, 'to_account': DST,
        'amount': 18, 'transaction_id': txid,
    })
    assert code == 409 and bad.get('error', {}).get('code') == 'TRANSACTION_ID_CONFLICT', (code, bad)
    assert (balance(SRC), balance(DST)) == (init[0] - 17, init[1] + 17)
    print('PASS: same transaction_id with different amount rejected')

    transfer(DST, SRC, 17)
    assert (balance(SRC), balance(DST)) == init, 'Fixture was not restored'
    print('PASS: reverse cross-shard transfer restored balances, sum preserved')
    check_prepared()
    print('PASS: Phase 5 real PostgreSQL 2PC acceptance completed')


if __name__ == '__main__':
    check()
