from __future__ import annotations
from dataclasses import replace
from uuid import uuid4

import pytest

from bank_platform.migration.control_plane import (DurableMigrationJournal,
    MigrationError, MigrationPlan, Observation, Step, new_plan)
from bank_platform.migration.replay_contract import AccountRevision, ReplayError, RevisionReplay


def journal(tmp_path):
    j = DurableMigrationJournal(tmp_path/'state.sqlite')
    j.initialize(new_plan())
    return j


def test_new_plan_is_immutable_and_can_be_reopened(tmp_path):
    j = journal(tmp_path)
    p = j.plan()
    assert p.first == 'A1501' and p.last == 'A2000' and p.source == 'B'
    j.initialize(p)
    with pytest.raises(MigrationError, match='Immutable'):
        j.initialize(replace(p, new_epoch=4))
    assert j.status()['phase'] == Step.PLANNED.value


@pytest.mark.parametrize('change', [
    {'source':'A'}, {'target':'B'}, {'first':'A1401'}, {'last':'A1999'},
    {'account_count':499}, {'old_epoch':2, 'new_epoch':2}, {'new_epoch':True},
])
def test_reject_invalid_plans(change):
    p = replace(new_plan(), **change)
    with pytest.raises(MigrationError):p.validate()


def test_pending_intent_survives_process_restart_and_is_not_duplicated(tmp_path):
    j = journal(tmp_path)
    token = j.begin(Step.SNAPSHOTTING)
    restarted = DurableMigrationJournal(j.path)
    assert restarted.begin(Step.SNAPSHOTTING) == token
    with pytest.raises(MigrationError,match='Unreconciled'):restarted.assert_no_unreconciled_intent()
    with pytest.raises(MigrationError,match='unresolved'):restarted.begin(Step.CATCHING_UP)
    assert restarted.status()['phase'] == 'PLANNED'


def obs(step=Step.SNAPSHOTTING, **changes):
    after = step in (Step.ACTIVATED, Step.VERIFYING, Step.COMPLETED)
    base = Observation(capture_enabled=True, snapshot_persisted=True,
                       target_standby=not after, source_fenced=True, target_active=after,
                       map_published=after, gateway_epoch=2 if after else 1, moved_accounts=500,
                       source_digest='a'*64, target_digest='a'*64,
                       prior_owner_rows=0 if step==Step.COMPLETED else 500)
    return replace(base, **changes)


def test_end_to_end_state_progression_with_recorded_proofs(tmp_path):
    j = journal(tmp_path)
    for step in (Step.SNAPSHOTTING, Step.CATCHING_UP, Step.FENCED,
                 Step.ACTIVATED, Step.VERIFYING, Step.COMPLETED):
        t = j.begin(step)
        result = j.confirm(t,obs(step))
        assert result['phase'] == step.value
        assert len(result['evidence_sha256'])==64
    assert j.status()['phase'] == 'COMPLETED'
    assert len(j.history()) == 6
    j.assert_no_unreconciled_intent()
    with pytest.raises(MigrationError):j.begin(Step.SNAPSHOTTING)


def test_wrong_step_rejected_and_confirmation_token_is_single_use(tmp_path):
    j = journal(tmp_path)
    with pytest.raises(MigrationError):j.begin(Step.ACTIVATED)
    t = j.begin(Step.SNAPSHOTTING)
    with pytest.raises(MigrationError):j.confirm(str(uuid4()),obs())
    j.confirm(t,obs())
    with pytest.raises(MigrationError):j.confirm(t,obs())


def test_missing_observation_never_implicitly_commits(tmp_path):
    j = journal(tmp_path)
    t=j.begin(Step.SNAPSHOTTING)
    with pytest.raises(MigrationError):j.confirm(t,obs(capture_enabled=False))
    assert j.status()['pending_id'] == t
    assert j.status()['phase'] == 'PLANNED'


@pytest.mark.parametrize('problem',[
    {'source_fenced':False}, {'source_prepared':1}, {'target_prepared':1},
    {'coordinator_pending':1}, {'lagging_accounts':2}, {'moved_accounts':499},
    {'target_digest':'b'*64}, {'target_standby':False},
    {'source_epoch':2}, {'target_epoch':1},
])
def test_fence_refuses_unsafe_readbacks(tmp_path, problem):
    j = journal(tmp_path)
    for step in (Step.SNAPSHOTTING,Step.CATCHING_UP):
        j.confirm(j.begin(step),obs(step))
    token=j.begin(Step.FENCED)
    with pytest.raises(MigrationError):j.confirm(token,obs(**problem))
    assert j.status()['phase']=='CATCHING_UP'


@pytest.mark.parametrize('problem',[
    {'target_active':False}, {'map_published':False}, {'gateway_epoch':1},
    {'source_fenced':False}, {'coordinator_pending':1},
])
def test_activation_requires_durable_cutover(tmp_path, problem):
    j = journal(tmp_path)
    for step in (Step.SNAPSHOTTING,Step.CATCHING_UP,Step.FENCED):
        j.confirm(j.begin(step),obs(step))
    token=j.begin(Step.ACTIVATED)
    with pytest.raises(MigrationError):j.confirm(token,obs(Step.ACTIVATED, **problem))
    assert j.status()['phase']=='FENCED'


def test_complete_requires_cleanup(tmp_path):
    j=journal(tmp_path)
    for step in (Step.SNAPSHOTTING,Step.CATCHING_UP,Step.FENCED,Step.ACTIVATED,Step.VERIFYING):
        j.confirm(j.begin(step),obs(step))
    token=j.begin(Step.COMPLETED)
    with pytest.raises(MigrationError, match='Old owner'):
        j.confirm(token,obs(Step.COMPLETED, prior_owner_rows=500))
    j.confirm(token,obs(Step.COMPLETED))


def test_corrupt_observation_types_are_rejected(tmp_path):
    j=journal(tmp_path)
    t=j.begin(Step.SNAPSHOTTING)
    with pytest.raises(MigrationError):j.confirm(t,obs(capture_enabled=1))
    with pytest.raises(MigrationError):j.confirm(t,obs(lagging_accounts=True))


def test_pre_cutover_cannot_publish_new_owner(tmp_path):
    j=journal(tmp_path)
    t=j.begin(Step.SNAPSHOTTING)
    with pytest.raises(MigrationError,match='Never publish'):
        j.confirm(t,obs(Step.ACTIVATED))


def test_replay_snapshot_and_idempotent_change():
    r=RevisionReplay()
    first=AccountRevision('A1501',1000,0)
    assert r.snapshot(first)
    assert not r.snapshot(first)
    assert r.apply(AccountRevision('A1501',1001,1))
    assert not r.apply(AccountRevision('A1501',1001,1))
    assert not r.snapshot(first)
    with pytest.raises(ReplayError):r.apply(AccountRevision('A1501',999,1))


def test_replay_gaps_and_conflicting_snapshots_rejected():
    r=RevisionReplay()
    r.snapshot(AccountRevision('A1501',1000,0))
    with pytest.raises(ReplayError,match='gap'):r.apply(AccountRevision('A1501',1004,2))
    with pytest.raises(ReplayError,match='conflicting'):r.snapshot(AccountRevision('A1501',1050,0))
    with pytest.raises(ReplayError,match='missing'):r.apply(AccountRevision('A1600',100,1))
    assert r.rows()[0].balance==1000


def test_replay_all_500_accounts_and_digest():
    r=RevisionReplay()
    initial=[AccountRevision(f'A{n:04d}',1000,0) for n in range(1501,2001)]
    for row in initial:r.snapshot(row)
    result=r.verify(initial)
    assert len(result)==64
    assert r.apply(AccountRevision('A1601',990,1))
    updated=[AccountRevision(x.account_id,990,1) if x.account_id=='A1601' else x for x in initial]
    assert r.verify(updated)!=result
    with pytest.raises(ReplayError):r.verify(initial)


def test_replay_does_not_trust_commit_order_of_global_event_sequence():
    # Commit order may differ from ID allocation order, even in PostgreSQL.
    r=RevisionReplay()
    r.snapshot(AccountRevision('A1601',1000,0))
    r.snapshot(AccountRevision('A1602',1000,0))
    # Event number 8 was committed before event number 7 for a *different* account.
    assert r.apply(AccountRevision('A1602',1005,1))
    assert r.apply(AccountRevision('A1601',995,1))
    assert (r.rows()[0].balance,r.rows()[1].balance)==(995,1005)


def test_replay_rejects_invalid_account_and_values():
    r=RevisionReplay()
    for row in (AccountRevision('A1200',1000,0),AccountRevision('A1601',-5,0),
                AccountRevision('A1601',1,True),AccountRevision('A1601',1,1.0)):
        with pytest.raises((ReplayError,ValueError)):
            r.snapshot(row)


def test_bad_digest_shape_rejected_before_fence(tmp_path):
    j=journal(tmp_path)
    for step in (Step.SNAPSHOTTING,Step.CATCHING_UP):
        j.confirm(j.begin(step),obs(step))
    t=j.begin(Step.FENCED)
    with pytest.raises(MigrationError,match='digests'):
        j.confirm(t,obs(Step.FENCED,source_digest='not-a-digest',target_digest='not-a-digest'))


def test_offline_preview_cannot_advance_state_with_cli(tmp_path):
    import subprocess, sys
    root=__import__('pathlib').Path(__file__).resolve().parents[1]
    tool=root/'scripts/bonus10e3_control_preview.py'
    state=tmp_path/'durable.sqlite'
    run=subprocess.run([sys.executable,str(tool),'--journal',str(state),'--init'],cwd=root,
                       text=True,capture_output=True)
    assert run.returncode==0,run.stderr
    assert 'NO_LIVE_MIGRATION' in run.stdout
    status=DurableMigrationJournal(state).status()
    assert status['phase']=='PLANNED' and status['pending_id'] is None
    assert subprocess.run([sys.executable,str(tool),'--journal',str(state),'--init'],cwd=root,
                          capture_output=True).returncode!=0
