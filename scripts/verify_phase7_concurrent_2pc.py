#!/usr/bin/env python3
"""Required experiment 7: overlapping cross-shard transfers on real PostgreSQL.

Only the Gateway is contacted. Every transfer has a stable UUIDv4 and its
recorded outcome is queried before accounting. On UNKNOWN/PENDING, STOP: no
blind retry and no compensating transfers until an operator resolves it.

On success, confirmed COMMITs are reversed with distinct durable transaction
IDs and four dedicated test accounts return to their initial balances.
"""
from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import sys
import time
import urllib.error
import urllib.request
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

DEFAULT_BASE = 'http://127.0.0.1:8001'
ACCOUNTS = ('A0901', 'A0902', 'A1901', 'A1902')
PAIRS = (
    ('A0901', 'A1901'), ('A1901', 'A0901'),
    ('A0901', 'A1902'), ('A1902', 'A0901'),
    ('A0902', 'A1901'), ('A1901', 'A0902'),
    ('A0902', 'A1902'), ('A1902', 'A0902'),
)


class VerificationHalt(RuntimeError):
    """Safety stop: inspect transaction status before any new writes."""


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def http_request(base, path, payload=None, timeout=38):
    body = json.dumps(payload).encode('utf-8') if payload is not None else None
    request = urllib.request.Request(base + path, data=body,
        headers={'Content-Type': 'application/json'},
        method='POST' if body is not None else 'GET')
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as exc:
        try:
            return exc.code, json.load(exc)
        except (ValueError, OSError):
            return exc.code, {'error': 'NON_JSON_HTTP_RESPONSE'}
    except (urllib.error.URLError, ConnectionError, TimeoutError, OSError, ValueError) as exc:
        return None, {'error': type(exc).__name__, 'detail': str(exc)}


def get_balance(base, account):
    code, value = http_request(base, '/api/v1/accounts/' + account + '/balance', timeout=15)
    if code != 200 or value.get('account') != account or type(value.get('balance')) is not int:
        raise VerificationHalt(f'Cannot read {account}: HTTP {code} {value}')
    expected_shard = 'A' if int(account[1:]) <= 1000 else 'B'
    if value.get('shard') != expected_shard or value.get('replica') not in (expected_shard + '1', expected_shard + '2'):
        raise VerificationHalt(f'Wrong shard/replica for {account}: {value}')
    return value['balance']


def balances(base):
    return {account: get_balance(base, account) for account in ACCOUNTS}


def get_tx(base, txid):
    return http_request(base, '/api/v1/transactions/' + txid, timeout=12)


def post_transfer(base, item):
    start = time.monotonic()
    code, reply = http_request(base, '/api/v1/operations', {
        'action': 'transfer', 'from_account': item['source'],
        'to_account': item['destination'], 'amount': item['amount'],
        'transaction_id': item['transaction_id'],
    })
    return {'transaction_id': item['transaction_id'], 'source': item['source'],
            'destination': item['destination'], 'amount': item['amount'],
            'http_status': code, 'gateway_response': reply,
            'duration_seconds': round(time.monotonic() - start, 3)}


def settle(base, item, deadline_seconds=90):
    """Read-only resolution; never resend a write when its outcome is unclear."""
    deadline = time.monotonic() + deadline_seconds
    last = None
    while time.monotonic() < deadline:
        code, state = get_tx(base, item['transaction_id'])
        last = {'http_status': code, 'body': state}
        if code == 200 and state.get('status') in ('COMMITTED', 'ABORTED'):
            if state.get('decision') != ('COMMIT' if state['status'] == 'COMMITTED' else 'ABORT'):
                raise VerificationHalt(f'Inconsistent decision txid={item["transaction_id"]}: {state}')
            if set(state.get('acknowledged_shards', ())) != {'A', 'B'}:
                raise VerificationHalt(f'Missing acknowledgements txid={item["transaction_id"]}: {state}')
            return state
        time.sleep(0.6)
    raise VerificationHalt(f'PENDING/UNKNOWN txid={item["transaction_id"]} last={last}. STOP; inspect Coordinator, do not use a new ID')


def expected_balances(initial, results):
    expected = dict(initial)
    for result in results:
        if result.get('final_status') == 'COMMITTED':
            expected[result['source']] -= result['amount']
            expected[result['destination']] += result['amount']
    return expected


def assert_balance_state(actual, expected):
    if actual != expected:
        raise VerificationHalt(f'Balance mismatch actual={actual}, expected={expected}; STOP')
    if sum(actual.values()) != sum(expected.values()):
        raise VerificationHalt('Conservation violation: sums differ; STOP')


def json_save(path, payload):
    if not path:
        return
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + '.part')
    tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True) + '\n', encoding='utf-8')
    os.replace(tmp, target)


def check(base=DEFAULT_BASE, *, count=16, workers=3, evidence=None):
    if count < 8 or count > 64 or count % len(PAIRS):
        raise ValueError('Count must be a multiple of 8 between 8 and 64')
    if workers < 2 or workers > 6:
        raise ValueError('Workers must be 2..6 to demonstrate concurrency safely')
    report = {'experiment': 7, 'title': 'Concurrent cross-shard transfers with overlapping accounts',
              'started_utc': utc_now(), 'base': base, 'workers': workers, 'count': count,
              'accounts': list(ACCOUNTS), 'operations': [], 'reversals': [], 'status': 'IN_PROGRESS'}
    try:
        code, ready = http_request(base, '/ready')
        if code != 200 or ready.get('healthy_replicas') != {'A': 2, 'B': 2}:
            raise VerificationHalt(f'All 4 replicas required: {code} {ready}')
        initial = balances(base)
        report['initial_balances'] = initial
        if min(initial.values()) < count * 3:
            raise VerificationHalt(f'Insufficient test fixture balances {initial}')
        print(f'Experiment 7 baseline: {initial}; workers={workers}; transfers={count}', flush=True)
        tasks = [{'transaction_id': str(uuid4()), 'source': PAIRS[i % 8][0],
                  'destination': PAIRS[i % 8][1], 'amount': 2 + (i % 3)}
                 for i in range(count)]
        # Preserve UUIDs in evidence BEFORE the first POST for manual recovery.
        report['planned_transactions'] = tasks
        json_save(evidence, report)

        # Every task overlaps at least one account with others; no client-side
        # serialization across transaction IDs. PostgreSQL/2PC must arbitrate.
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(post_transfer, base, task) for task in tasks]
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                report['operations'].append(result)
                json_save(evidence, report)

        # POST 409 can legitimately mean a durable ABORT under contention.
        # No assumed COMMIT on 200 and no blind retry on 202/504.
        for item in report['operations']:
            state = settle(base, item)
            item['final_status'] = state['status']
            item['decision'] = state['decision']
            item['acknowledged_shards'] = state['acknowledged_shards']
            json_save(evidence, report)

        totals = Counter(x['final_status'] for x in report['operations'])
        report['outcomes'] = dict(totals)
        commits = [x for x in report['operations'] if x['final_status'] == 'COMMITTED']
        if len(commits) < 2:
            raise VerificationHalt(f'Insufficient successful overlapping commits: {dict(totals)}')
        if len({(x['source'], x['destination']) for x in commits}) < 2:
            raise VerificationHalt('Successful commits did not use diverse account directions')

        expected = expected_balances(initial, report['operations'])
        actual = balances(base)
        report['after_workload_balances'] = actual
        report['expected_after_workload'] = expected
        assert_balance_state(actual, expected)
        if sum(initial.values()) != sum(actual.values()):
            raise VerificationHalt('Money conservation failed across all four accounts')
        print(f'PASS: {len(commits)} COMMITTED, {totals["ABORTED"]} durable ABORTED, every UUID resolved', flush=True)
        print(f'PASS: exact account balances & total conserved: initial={initial} actual={actual}', flush=True)
        json_save(evidence, report)

        # Replay a committed UUID with identical payload; must not reapply.
        prior = commits[0]
        repeat = post_transfer(base, prior)
        repeat_state = settle(base, prior)
        if repeat['http_status'] != 200 or repeat_state['status'] != 'COMMITTED':
            raise VerificationHalt(f'Idempotent replay failed {prior["transaction_id"]}: {repeat}')
        assert_balance_state(balances(base), expected)
        print('PASS: replay of an original committed UUID did not duplicate money movement', flush=True)

        # Cleanup happens ONLY if all original transaction IDs have resolved.
        # Never 'reset' balances with SQL or use a retry of an uncertain write.
        for old in reversed(commits):
            reverse = {'transaction_id': str(uuid4()), 'source': old['destination'],
                       'destination': old['source'], 'amount': old['amount']}
            report['reversals'].append({**reverse, 'state': 'PLANNED'})
            json_save(evidence, report)
            out = post_transfer(base, reverse)
            report['reversals'][-1].update(out)
            json_save(evidence, report)
            outcome = settle(base, reverse)
            report['reversals'][-1]['final_status'] = outcome['status']
            json_save(evidence, report)
            if outcome['status'] != 'COMMITTED':
                raise VerificationHalt(f'Reversal not committed txid={reverse["transaction_id"]}: {outcome}; STOP')
        final = balances(base)
        report['final_balances'] = final
        assert_balance_state(final, initial)
        report['status'] = 'PASS'
        print(f'PASS: every original committed transfer compensated, final balances restored: {final}', flush=True)
        print('PASS: Experiment 7 overlapping cross-shard concurrency complete', flush=True)
        return report
    except BaseException as exc:
        report['status'] = 'FAIL_STOP' if isinstance(exc, Exception) else 'INTERRUPTED'
        report['failure'] = str(exc)
        raise
    finally:
        report['finished_utc'] = utc_now()
        json_save(evidence, report)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base-url', default=DEFAULT_BASE)
    parser.add_argument('--count', type=int, default=16)
    parser.add_argument('--workers', type=int, default=3)
    parser.add_argument('--evidence', help='Optional JSON evidence output (contains every transaction UUID)')
    args = parser.parse_args()
    try:
        check(args.base_url.rstrip('/'), count=args.count, workers=args.workers, evidence=args.evidence)
    except (VerificationHalt, RuntimeError, AssertionError, ValueError) as exc:
        print(f'FAIL/STOP: {exc}', file=sys.stderr, flush=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
