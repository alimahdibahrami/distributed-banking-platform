"""B10E4A source-only contract tests. No production PostgreSQL connection."""
from __future__ import annotations
from contextlib import nullcontext
from pathlib import Path
import pytest
from bank_platform.migration.replay_contract import AccountRevision
from bank_platform.migration.staging_contract import (
    StagedRow, ReplayReceipt, StageConflict, accept_snapshot, accept_change, digest,
)
from bank_platform.migration.postgres_staging import PostgreSQLStaging, StagingDatabaseError


def ar(rev=0, bal=1000, account='A1501'):
    return AccountRevision(account,bal,rev)


def stage(rev=0, bal=1000, base_rev=0, base_bal=1000):
    return StagedRow('A1501',bal,rev,base_rev,base_bal)


def test_initial_snapshot_is_atomic_logical_insert():
    new, change = accept_snapshot(None, ar(1,999))
    assert change and new == stage(1,999,1,999)


def test_snapshot_is_idempotent():
    item=stage(1,999,1,999)
    assert accept_snapshot(item,ar(1,999)) == (item,False)


def test_snapshot_of_older_base_can_be_rechecked_after_replay():
    item=stage(2,995,1,999)
    assert accept_snapshot(item,ar(1,999)) == (item,False)


@pytest.mark.parametrize('incoming', [ar(1,998),ar(0,999),ar(2,1001)])
def test_snapshot_rejects_changed_checkpoint(incoming):
    with pytest.raises(StageConflict): accept_snapshot(stage(1,999,1,999),incoming)


def test_snapshot_rejects_account_mismatch():
    with pytest.raises(StageConflict): accept_snapshot(stage(),ar(account='A1502'))


def test_replay_first_event():
    new,receipt,changed=accept_change(stage(),None,ar(1,1010))
    assert changed and new == stage(1,1010)
    assert receipt == ReplayReceipt('A1501',1010,1)


def test_replay_duplicate_receipt():
    current=stage(2,1002)
    old_receipt=ReplayReceipt('A1501',1001,1)
    assert accept_change(current,old_receipt,ar(1,1001)) == (current,old_receipt,False)


def test_replay_conflicting_receipt_fails():
    with pytest.raises(StageConflict):
        accept_change(stage(1,1001),ReplayReceipt('A1501',1001,1),ar(1,999))


def test_replay_receipt_without_state_fails():
    with pytest.raises(StageConflict):
        accept_change(stage(),ReplayReceipt('A1501',1001,1),ar(1,1001))


def test_replay_captured_in_snapshot_is_not_reapplied():
    item=stage(3,1040,3,1040)
    assert accept_change(item,None,ar(2,1030)) == (item,None,False)


def test_replay_hole_fails():
    with pytest.raises(StageConflict): accept_change(stage(),None,ar(2,1001))


def test_replay_missing_receipt_fails():
    with pytest.raises(StageConflict): accept_change(stage(2,1001),None,ar(1,1000))


def test_replay_needs_snapshot():
    with pytest.raises(StageConflict): accept_change(None,None,ar(1,1000))


def test_replay_rejects_revision_zero():
    with pytest.raises(StageConflict): accept_change(stage(),None,ar(0,1000))


def test_replay_rejects_wrong_account():
    with pytest.raises(StageConflict): accept_change(stage(),None,ar(1,1000,'A1600'))


def test_staged_row_validation_rejects_bad_base():
    with pytest.raises(StageConflict): stage(0,1000,1,1000).validate()


def test_500_row_digest_is_deterministic():
    rows=[AccountRevision(f'A{i:04d}',1000,0) for i in range(1501,2001)]
    assert digest(rows)==digest(list(reversed(rows)))
    assert digest(rows)!=digest(rows[:-1]+[ar(1,999,'A2000')])


def test_digest_fails_when_one_missing():
    with pytest.raises(StageConflict): digest([AccountRevision(f'A{i:04d}',1000,0) for i in range(1501,2000)])


def test_digest_fails_on_duplicate():
    rows=[AccountRevision(f'A{i:04d}',1000,0) for i in range(1501,2001)]
    with pytest.raises(StageConflict): digest(rows+[rows[-1]])


def test_staging_schema_only_adds_tables():
    p=Path(__file__).resolve().parents[1]/'database/bonus10e4_target_staging.sql'
    s=p.read_text()
    assert 'CREATE TABLE IF NOT EXISTS bank_migration_stage_accounts' in s
    assert 'CREATE TABLE IF NOT EXISTS bank_migration_stage_receipts' in s
    for dangerous in ('UPDATE accounts','DELETE FROM accounts','DROP TABLE accounts',
                      'ALTER TABLE accounts','UPDATE bank_migration_fence'):
        assert dangerous not in s


def test_worker_source_is_repeatable_read_without_event_id_cursor():
    import inspect
    s=inspect.getsource(PostgreSQLStaging)
    assert 'REPEATABLE READ, READ ONLY' in s
    assert 'ORDER BY account_id,account_revision' in s
    assert 'MAX(event_id)' not in s
    assert 'UPDATE accounts' not in s
    assert 'DELETE FROM accounts' not in s


class _Result:
    def __init__(self, row=None, rows=None): self.row,self.rows=row,rows
    def fetchone(self): return self.row
    def fetchall(self): return self.rows


class _FakeSource:
    def __init__(self,gate): self.gate=gate;self.sql=[]
    def transaction(self): return nullcontext()
    def execute(self,query,args=None):
        self.sql.append(query)
        if 'FROM bank_migration_fence' in query: return _Result(self.gate)
        if 'pg_prepared_xacts' in query: return _Result((0,))
        if 'FROM accounts' in query: return _Result(rows=[(f'A{i:04d}',1000,0) for i in range(1501,2001)])
        return _Result()


def test_source_gate_must_capture():
    src=_FakeSource(('B',1,False,False))
    with pytest.raises(StagingDatabaseError): PostgreSQLStaging(src,None)._read_source()


def test_source_gate_rejects_wrong_owner():
    src=_FakeSource(('C',2,True,True))
    with pytest.raises(StagingDatabaseError): PostgreSQLStaging(src,None)._read_source()


def test_source_gate_valid_before_fencing():
    src=_FakeSource(('B',1,False,True))
    result=PostgreSQLStaging(src,None)._read_source()
    assert len(result)==500


def test_fenced_read_rejects_unfenced_source():
    src=_FakeSource(('B',1,False,True))
    with pytest.raises(StagingDatabaseError): PostgreSQLStaging(src,None)._read_source(fenced=True)


def test_fenced_read_requires_owner_c_epoch_2():
    src=_FakeSource(('C',2,True,True))
    assert len(PostgreSQLStaging(src,None)._read_source(fenced=True))==500
