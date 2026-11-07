"""Phase 7 harness tests do not contact Docker or modify real account balances."""
import json
import threading

import pytest

from scripts import verify_phase7 as runner
from scripts import verify_phase7_concurrent_2pc as workload


def test_all_seven_in_pdf_order_and_disruption_guard():
    assert list(runner.EXPERIMENTS) == list(range(1, 8))
    assert runner.DISRUPTIVE == {4, 6}
    runner.check_selected([1, 2, 3, 5, 7], False)
    with pytest.raises(ValueError, match='--allow-disruptive'):
        runner.check_selected([1, 4], False)
    with pytest.raises(ValueError, match='--allow-disruptive'):
        runner.check_selected([6], False)
    runner.check_selected(list(range(1, 8)), True)
    assert len(runner.EXPERIMENTS[7][1]) == 1


def test_expected_balances_count_only_durable_commits():
    before = {'A0901': 100, 'A1901': 50}
    attempts = [
        {'source': 'A0901', 'destination': 'A1901', 'amount': 7, 'final_status': 'COMMITTED'},
        {'source': 'A1901', 'destination': 'A0901', 'amount': 6, 'final_status': 'ABORTED'},
    ]
    after = workload.expected_balances(before, attempts)
    assert after == {'A0901': 93, 'A1901': 57}
    assert sum(after.values()) == sum(before.values())
    with pytest.raises(workload.VerificationHalt, match='mismatch'):
        workload.assert_balance_state({'A0901': 94, 'A1901': 56}, after)


class FakeGateway:
    def __init__(self, abort_alternate=False):
        self.bal = {a: 1000 for a in workload.ACCOUNTS}
        self.transactions = {}
        self.lock = threading.Lock()
        self.abort_alternate = abort_alternate

    def __call__(self, base, path, payload=None, timeout=38):
        if path == '/ready':
            return 200, {'healthy_replicas': {'A': 2, 'B': 2}}
        if path.startswith('/api/v1/accounts/'):
            account = path.split('/')[4]
            shard = 'A' if int(account[1:]) <= 1000 else 'B'
            with self.lock:
                amount = self.bal[account]
            return 200, {'account': account, 'balance': amount, 'shard': shard, 'replica': shard + '1'}
        if path.startswith('/api/v1/transactions/'):
            txid = path.rsplit('/', 1)[-1]
            with self.lock:
                return (200, dict(self.transactions[txid])) if txid in self.transactions else (404, {})
        if path == '/api/v1/operations':
            txid = payload['transaction_id']
            with self.lock:
                if txid in self.transactions:
                    tx = self.transactions[txid]
                    return (200 if tx['status'] == 'COMMITTED' else 409), dict(tx)
                index = len(self.transactions)
                abort = self.abort_alternate and index < 8 and index % 2 == 1
                if not abort:
                    self.bal[payload['from_account']] -= payload['amount']
                    self.bal[payload['to_account']] += payload['amount']
                tx = {'transaction_id': txid,
                      'status': 'ABORTED' if abort else 'COMMITTED',
                      'decision': 'ABORT' if abort else 'COMMIT',
                      'acknowledged_shards': ['A', 'B']}
                self.transactions[txid] = tx
                return (409 if abort else 200), dict(tx)
        raise AssertionError(path)


@pytest.mark.parametrize('abort_alternate', [False, True])
def test_concurrent_overlapping_workload_is_balance_neutral(monkeypatch, tmp_path, abort_alternate):
    gateway = FakeGateway(abort_alternate=abort_alternate)
    monkeypatch.setattr(workload, 'http_request', gateway)
    path = tmp_path / 'tx.json'
    result = workload.check(count=8, workers=2, evidence=str(path))
    assert result['status'] == 'PASS'
    assert result['initial_balances'] == result['final_balances'] == {a: 1000 for a in workload.ACCOUNTS}
    assert len(result['operations']) == 8
    assert result['outcomes']['COMMITTED'] >= 2
    assert len(result['reversals']) == result['outcomes']['COMMITTED']
    tids = [x['transaction_id'] for x in result['operations'] + result['reversals']]
    assert len(tids) == len(set(tids))
    stored = json.loads(path.read_text())
    assert stored['status'] == 'PASS'
    assert stored['after_workload_balances'] == result['after_workload_balances']


def test_ambiguous_transfer_halts_before_cleanup(monkeypatch, tmp_path):
    gateway = FakeGateway()
    monkeypatch.setattr(workload, 'http_request', gateway)
    def pending(*args, **kwargs):
        raise workload.VerificationHalt('PENDING/UNKNOWN UUID; STOP')
    monkeypatch.setattr(workload, 'settle', pending)
    path = tmp_path / 'stopped.json'
    with pytest.raises(workload.VerificationHalt, match='PENDING/UNKNOWN'):
        workload.check(count=8, workers=2, evidence=str(path))
    evidence = json.loads(path.read_text())
    assert evidence['status'] == 'FAIL_STOP'
    assert evidence['reversals'] == []
    assert len(evidence['planned_transactions']) == 8


def test_worker_safety_bounds():
    with pytest.raises(ValueError, match='multiple of 8'):
        workload.check(count=9, workers=2)
    with pytest.raises(ValueError, match='Workers must'):
        workload.check(count=8, workers=1)
