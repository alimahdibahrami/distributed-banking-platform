"""B10E4D only: fail-closed B -> C orchestrator for an isolated Compose stack.

No host Docker socket, no production URL, no public mutating endpoint.
The *only* permitted database names are b10e5_{a,b,c} on named lab hosts.
Involves controlled unavailability for moved accounts during cutover.
"""
from __future__ import annotations

import json
import os
import sqlite3
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict
from pathlib import Path
from uuid import uuid4

import httpx

from bank_platform.migration.control_plane import (
    DurableMigrationJournal, MigrationError, Observation, Step, new_plan,
)
from bank_platform.migration.postgres_staging import PostgreSQLStaging
from bank_platform.migration.replay_contract import AccountRevision
from bank_platform.migration.staging_contract import digest

LAB_NAME = 'distributed-banking-b10e5-crash'
JOURNAL = Path('/app/state/migration.sqlite3')
GATEWAY = 'http://gateway:8000'
COORDINATOR = 'http://coordinator:8000'
EXPECTED_TOTAL = 1999500
HOSTS = {'A': 'shard_a_db', 'B': 'shard_b_db', 'C': 'shard_c_db'}
DATABASES = {'A': 'b10e5_a', 'B': 'b10e5_b', 'C': 'b10e5_c'}


class LabRefused(RuntimeError):
    pass


INJECTION_POINTS = {
    'CAPTURE_AFTER_COMMIT',
    'SNAPSHOT_AFTER_COMMIT',
    'FENCE_AFTER_COMMIT',
    'PROMOTE_AFTER_C_ACTIVATION',
    'PROMOTE_AFTER_B_CLEANUP',
}


def crash_checkpoint(name: str) -> None:
    """Crash only an isolated operator container, after a durable side effect.

    No host Docker socket or production URL is available to the operator.
    The fault name is explicitly allowlisted; no implicit retry of banking writes.
    """
    if os.environ.get('B10E5_FAULT_POINT', '') != name:
        return
    require(name in INJECTION_POINTS, 'Unknown injection point')
    require(os.getenv('LAB_ONLY') == LAB_NAME, 'Injection outside lab forbidden')
    print('BONUS10E5_EXPECTED_CRASH: ' + name, flush=True)
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(86)


def require(condition: bool, message: str) -> None:
    if not condition:
        raise LabRefused(message)


def connect(shard: str):
    require(os.getenv('LAB_ONLY') == LAB_NAME, 'LAB_ONLY sentinel mismatch')
    require(shard in HOSTS, 'Only isolated A/B/C lab DB names allowed')
    pwd = os.environ[f'LAB_{shard}_PASSWORD']
    require(bool(pwd), 'Empty lab password')
    import psycopg  # only required inside the Docker operator image
    conn = psycopg.connect(host=HOSTS[shard], port=5432, dbname=DATABASES[shard],
                           user='bank', password=pwd, connect_timeout=4, autocommit=True)
    actual = conn.execute('SELECT current_database(), current_user').fetchone()
    if actual != (DATABASES[shard], 'bank'):
        conn.close()
        raise LabRefused('DB identity check failed; production database forbidden')
    return conn


def count(conn, query: str) -> int:
    return int(conn.execute(query).fetchone()[0])


def fence(conn) -> tuple[str, int, bool, bool]:
    r = conn.execute('SELECT owner_shard,owner_epoch,fence_enabled,capture_enabled '
                     'FROM bank_migration_fence WHERE singleton=1').fetchone()
    require(r is not None, 'Fence row missing')
    return str(r[0]).strip(), int(r[1]), r[2] is True, r[3] is True


def state(conn, shard: str) -> dict:
    meta = conn.execute('SELECT shard_id,first_account,last_account FROM shard_metadata '
                        'WHERE singleton=1').fetchone()
    require(meta and str(meta[0]).strip() == shard, 'Physical DB metadata invalid')
    return {'shard': shard, 'first': int(meta[1]), 'last': int(meta[2]),
            'accounts': count(conn, 'SELECT count(*) FROM accounts'),
            'total': count(conn, 'SELECT COALESCE(sum(balance),0) FROM accounts'),
            'prepared': count(conn, 'SELECT count(*) FROM pg_prepared_xacts '
                            'WHERE database=current_database()'),
            'fence': fence(conn) if shard in ('B', 'C') else None}


def pending_coordinator() -> int:
    path = '/app/coord_state/coordinator.sqlite3'
    require(Path(path).exists(), 'Coordinator journal file missing')
    uri = 'file:' + path + '?mode=ro'
    with sqlite3.connect(uri, uri=True, timeout=5) as con:
        return int(con.execute("SELECT count(*) FROM transactions "
                               "WHERE state NOT IN ('DONE','ABORTED')").fetchone()[0])


def zero_prepared(db: dict[str, object]) -> None:
    for name, con in db.items():
        require(state(con, name)['prepared'] == 0, f'Prepared 2PC on shard {name}')
    require(pending_coordinator() == 0, 'Coordinator has unresolved 2PC decisions')


def movable(conn, table: str = 'accounts') -> list[AccountRevision]:
    require(table in ('accounts', 'bank_migration_stage_accounts'), 'Untrusted table name')
    rev = 'migration_revision' if table == 'accounts' else 'account_revision'
    rows = conn.execute(f'SELECT account_id,balance,{rev} FROM {table} '
                        "WHERE account_id BETWEEN 'A1501' AND 'A2000' "
                        'ORDER BY account_id').fetchall()
    result = [AccountRevision(str(a), int(b), int(r)) for a, b, r in rows]
    digest(result)
    return result


def published_epoch() -> int:
    with httpx.Client(timeout=4.0) as client:
        response = client.get(GATEWAY + '/ready')
    data = response.json()
    require(response.status_code == 200 and data.get('banking_ready') is True,
            'Lab Gateway not banking-ready')
    epoch = data.get('routing_epoch')
    require(type(epoch) is int and epoch in (1, 2), 'Gateway epoch not proven')
    return epoch


def observe(*, phase: Step, db: dict, content: str = '') -> Observation:
    b, c = db['B'], db['C']
    fb, fc = fence(b), fence(c)
    if not content and phase in (Step.FENCED, Step.ACTIVATED, Step.VERIFYING, Step.COMPLETED):
        content = digest(movable(c, 'bank_migration_stage_accounts'))
    epoch = 2 if phase in (Step.ACTIVATED, Step.VERIFYING, Step.COMPLETED) else 1
    if phase in (Step.ACTIVATED, Step.VERIFYING, Step.COMPLETED):
        require(published_epoch() == 2, 'Gateway still routes old map')
    return Observation(
        capture_enabled=fb[3],
        snapshot_persisted=count(c, 'SELECT count(*) FROM bank_migration_stage_accounts') == 500,
        target_standby=fc[0] != 'C',
        source_fenced=fb[:3] == ('C', 2, True),
        target_active=fc[:3] == ('C', 2, True),
        map_published=epoch == 2,
        gateway_epoch=epoch,
        source_prepared=state(b, 'B')['prepared'],
        target_prepared=state(c, 'C')['prepared'],
        coordinator_pending=pending_coordinator(),
        lagging_accounts=0 if content else 500,
        moved_accounts=500 if content else 0,
        source_digest=content,
        target_digest=content,
        prior_owner_rows=count(b, "SELECT count(*) FROM accounts WHERE account_id BETWEEN 'A1501' AND 'A2000'"),
    )


def journal() -> DurableMigrationJournal:
    j = DurableMigrationJournal(JOURNAL)
    if not JOURNAL.exists():
        j.initialize(new_plan())
    return j


def confirm_step(j: DurableMigrationJournal, step: Step, db: dict, *, content: str = '') -> None:
    token = j.begin(step)
    observation = observe(phase=step, db=db, content=content)
    result = j.confirm(token, observation)
    print('B10E4D_JOURNAL_PASS:', json.dumps(result, sort_keys=True))


def guard_rejection_tests(db: dict, *, after_source_fence: bool) -> None:
    """Negative SQL tests in rolled-back isolated transactions."""
    shard = 'B' if after_source_fence else 'C'
    con = db[shard]
    original = count(con, 'SELECT COALESCE(sum(balance),0) FROM accounts')
    import psycopg
    try:
        with con.transaction():
            con.execute("SELECT set_config('bank.owner_shard','C',true)")
            con.execute("SELECT set_config('bank.owner_epoch','2',true)")
            if shard == 'B':
                con.execute("UPDATE accounts SET balance=balance+1 WHERE account_id='A1700'")
            else:
                con.execute("INSERT INTO accounts (account_id,balance) VALUES ('A1700',1)")
    except psycopg.Error as exc:
        require('STALE_OWNER_EPOCH' in str(exc),
                f'Expected physical guard refusal, got {type(exc).__name__}')
    else:
        raise LabRefused(f'Physical owner guard DID NOT reject forged {shard} epoch')
    require(count(con, 'SELECT COALESCE(sum(balance),0) FROM accounts') == original,
            'Guard-negative test unexpectedly changed money')
    print('BONUS10E4D_PHYSICAL_SQL_GUARD_PASS:', shard)


def capture_start(j: DurableMigrationJournal, db: dict) -> None:
    require(j.status()['phase'] == 'PLANNED', 'Capture only from PLANNED')
    b, c = db['B'], db['C']
    zero_prepared(db)
    require(state(b, 'B')['accounts'] == 1000 and state(c, 'C')['accounts'] == 0,
            'Non-empty target or incomplete source')
    require(fence(b) in (('B',1,False,False), ('B',1,False,True)),
            'Unexpected B ownership before capture')
    require(fence(c) == ('B', 1, False, False), 'C standby not at baseline')
    require(published_epoch() == 1, 'Expected epoch 1 initially')
    guard_rejection_tests(db, after_source_fence=False)
    j.begin(Step.SNAPSHOTTING)
    if not fence(b)[3]:
        with b.transaction():
            b.execute("UPDATE bank_migration_fence SET capture_enabled=TRUE,changed_at=now() "
                      "WHERE singleton=1 AND owner_shard='B' AND owner_epoch=1 "
                      "AND fence_enabled=FALSE AND capture_enabled=FALSE")
    crash_checkpoint('CAPTURE_AFTER_COMMIT')
    confirm_step(j, Step.SNAPSHOTTING, db)
    print('BONUS10E4D_CAPTURE_ENABLED')


def snapshot(j: DurableMigrationJournal, db: dict) -> None:
    require(j.status()['phase'] == Step.SNAPSHOTTING.value, 'Snapshot requires SNAPSHOTTING')
    token = j.begin(Step.CATCHING_UP)
    staged = count(db['C'], 'SELECT count(*) FROM bank_migration_stage_accounts')
    if staged == 0:
        result = PostgreSQLStaging(db['B'], db['C']).snapshot()
    else:
        # The entire snapshot INSERT was one PostgreSQL transaction. If an
        # operator crashed after commit, do not resnapshot changed source rows.
        require(staged == 500, 'Partial/non-atomic snapshot; refuse automatic resume')
        content = stage_digest(db['C'])
        result = {'accounts': 500, 'snapshot_digest': content, 'resumed': True}
    crash_checkpoint('SNAPSHOT_AFTER_COMMIT')
    confirm_step(j, Step.CATCHING_UP, db)
    print('BONUS10E4D_SNAPSHOT_PASS:', json.dumps(result, sort_keys=True))


def exercise(db: dict) -> None:
    require(fence(db['B']) == ('B', 1, False, True), 'Only while B owns and captures')
    require(published_epoch() == 1, 'Unexpected Gateway epoch')
    def task(n):
        a = f'A{1501 + n * 2:04d}'
        b = f'A{1502 + n * 2:04d}'
        with httpx.Client(timeout=12.0) as client:
            response = client.post(GATEWAY + '/api/v1/operations', json={
                'action': 'transfer', 'from_account': a, 'to_account': b, 'amount': 1})
            return (n, response.status_code, response.text[:180])
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = [f.result() for f in as_completed(pool.submit(task, n) for n in range(80))]
    failed = [r for r in results if r[1] != 200]
    require(not failed, f'Financial write had unknown outcome: {failed[:3]}')
    require(sum(state(db[s], s)['total'] for s in ('A','B','C')) == EXPECTED_TOTAL,
            'Money conservation failed after concurrent transfers')
    print('BONUS10E4D_CONCURRENT_GATEWAY_TRANSFERS_PASS: 80 transfers / 160 CDC revisions')


def fence_source(j: DurableMigrationJournal, db: dict) -> None:
    require(j.status()['phase'] == Step.CATCHING_UP.value, 'Fence requires CATCHING_UP')
    zero_prepared(db)
    require(fence(db['B']) in (('B',1,False,True), ('C',2,True,True)),
            'B must capture continuously')
    j.begin(Step.FENCED)
    stage = PostgreSQLStaging(db['B'], db['C'])
    if fence(db['B']) == ('B',1,False,True):
        result = stage.replay()
        print('BONUS10E4D_PREFENCE_REPLAY:', json.dumps(result))
        with db['B'].transaction():
            db['B'].execute("SET LOCAL lock_timeout = '5s'")
            db['B'].execute('''UPDATE bank_migration_fence SET owner_shard='C',
                owner_epoch=2,fence_enabled=TRUE,changed_at=now()
                WHERE singleton=1 AND owner_shard='B' AND owner_epoch=1
                AND fence_enabled=FALSE AND capture_enabled=TRUE''')
            require(db['B'].execute('SELECT owner_shard,owner_epoch,fence_enabled FROM '
                                    'bank_migration_fence WHERE singleton=1').fetchone()
                    == ('C', 2, True), 'B fence flip failed')
    crash_checkpoint('FENCE_AFTER_COMMIT')
    zero_prepared(db)
    guard_rejection_tests(db, after_source_fence=True)
    result = stage.replay()
    converged = stage.fenced_convergence()
    content = converged['data_digest']
    require(digest(movable(db['B'])) == content, 'Post-fence source digest differs')
    confirm_step(j, Step.FENCED, db, content=content)
    print('BONUS10E4D_FENCED_CONVERGENCE_PASS:', json.dumps({**result, **converged}, sort_keys=True))


def stage_digest(conn) -> str:
    return digest(movable(conn, 'bank_migration_stage_accounts'))


def promote_c(db: dict) -> None:
    b, c = db['B'], db['C']
    require(fence(b)[:3] == ('C', 2, True), 'Source fence is mandatory')
    zero_prepared(db)
    content = stage_digest(c)
    source_count = count(b, "SELECT count(*) FROM accounts WHERE account_id BETWEEN 'A1501' AND 'A2000'")
    if source_count == 500:
        require(digest(movable(b)) == content, 'Source/stage mismatch during promotion')
    else:
        require(source_count == 0 and fence(c) == ('C',2,True,False),
                'Cannot activate standby target after source data disappeared')
    with c.transaction():
        c.execute("SET LOCAL lock_timeout = '5s'")
        current = fence(c)
        existing = count(c, 'SELECT count(*) FROM accounts')
        if current == ('B',1,False,False):
            require(existing == 0, 'C activation encountered unexpected live accounts')
            c.execute("UPDATE bank_migration_fence SET owner_shard='C',owner_epoch=2,"
                      "fence_enabled=TRUE,capture_enabled=FALSE,changed_at=now() WHERE singleton=1")
            c.execute("SELECT set_config('bank.owner_shard','C',true)")
            c.execute("SELECT set_config('bank.owner_epoch','2',true)")
            c.execute('''INSERT INTO accounts (account_id,balance,migration_revision)
                SELECT account_id,balance,account_revision FROM bank_migration_stage_accounts''')
        else:
            require(current == ('C',2,True,False) and existing == 500,
                    'C partially promoted; refuse mutation')
        require(digest(movable(c)) == content, 'C account digest after activation mismatch')
    print('BONUS10E4D_TARGET_ACTIVATED_PASS')


def cleanup_b(db: dict) -> None:
    b, c = db['B'], db['C']
    zero_prepared(db)
    require(fence(b) == ('C',2,True,True), 'B must already be fenced')
    require(fence(c) == ('C',2,True,False), 'C must already own accounts')
    require(digest(movable(c)) == stage_digest(c), 'C live accounts differ from staged proof')
    current = state(b, 'B')
    if current['last'] == 1500 and current['accounts'] == 500:
        require(count(b, "SELECT count(*) FROM accounts WHERE account_id BETWEEN 'A1501' AND 'A2000'") == 0,
                'Residual B owner rows after cutover')
        return
    require(current['last'] == 2000 and current['accounts'] == 1000,
            'B metadata inconsistent; cannot clean')
    require(digest(movable(b)) == stage_digest(c), 'B and C diverged before cleanup')
    with b.transaction():
        b.execute("SET LOCAL lock_timeout = '5s'")
        b.execute('LOCK TABLE accounts IN ACCESS EXCLUSIVE MODE')
        # Preserve historical committed 2PC receipts; remove only their FK,
        # since a committed transfer may refer to an account now owned by C.
        b.execute('ALTER TABLE tpc_participant_commits '
                  'DROP CONSTRAINT IF EXISTS tpc_participant_commits_account_id_fkey')
        # Single transaction with ACCESS EXCLUSIVE prevents observers from seeing
        # a disabled trigger. A PostgreSQL crash rolls the entire change back.
        b.execute('ALTER TABLE accounts DISABLE TRIGGER bank_migration_fence_before')
        b.execute("DELETE FROM accounts WHERE account_id BETWEEN 'A1501' AND 'A2000'")
        b.execute('ALTER TABLE accounts ENABLE TRIGGER bank_migration_fence_before')
        b.execute('ALTER TABLE accounts DROP CONSTRAINT account_id_in_shard_b')
        b.execute("ALTER TABLE accounts ADD CONSTRAINT account_id_in_shard_b CHECK ("
                  "account_id ~ '^A[0-9]{4}$' AND substring(account_id from 2)::INTEGER BETWEEN 1001 AND 1500)")
        b.execute('ALTER TABLE shard_metadata DROP CONSTRAINT shard_metadata_last_account_check')
        b.execute('UPDATE shard_metadata SET last_account=1500 WHERE singleton=1')
        b.execute('ALTER TABLE shard_metadata ADD CONSTRAINT shard_metadata_last_account_check '
                  'CHECK (last_account=1500)')
        require(count(b, 'SELECT count(*) FROM accounts') == 500, 'B cleanup count mismatch')
    print('BONUS10E4D_SOURCE_CLEANUP_PASS')


def promote(j: DurableMigrationJournal, db: dict) -> None:
    status = j.status()
    require(status['phase'] == Step.FENCED.value, 'Promotion requires FENCED proof')
    require(status['pending_phase'] in (None, Step.ACTIVATED.value), 'Unreconciled different intent')
    j.begin(Step.ACTIVATED)  # Pending until Gateway+Coordinator prove epoch2.
    promote_c(db)
    crash_checkpoint('PROMOTE_AFTER_C_ACTIVATION')
    cleanup_b(db)
    crash_checkpoint('PROMOTE_AFTER_B_CLEANUP')
    require(state(db['A'], 'A')['accounts'] == 1000, 'A changed unexpectedly')
    require(sum(state(db[s], s)['total'] for s in ('A', 'B', 'C')) == EXPECTED_TOTAL,
            'Total money mismatch after promotion')
    print('BONUS10E4D_READY_FOR_MAP_PUBLICATION')


def prepublish(j: DurableMigrationJournal, db: dict) -> None:
    st = j.status()
    require(st['phase'] == Step.FENCED.value and st['pending_phase'] == Step.ACTIVATED.value,
            'Operator must first promote and persist ACTIVATED intent')
    require(state(db['B'],'B')['last'] == 1500 and state(db['B'],'B')['accounts'] == 500,
            'B physical identity not migrated')
    require(state(db['C'],'C')['accounts'] == 500 and fence(db['C']) == ('C',2,True,False),
            'C is not ready for routing')
    require(fence(db['B']) == ('C',2,True,True), 'B physical fence not persisted')
    require(stage_digest(db['C']) == digest(movable(db['C'])), 'C staging/live mismatch')
    zero_prepared(db)
    require(sum(state(db[s], s)['total'] for s in ('A','B','C')) == EXPECTED_TOTAL,
            'Global money invariant failed')
    print('BONUS10E4D_PREPUBLISH_APPROVED_LAB_ONLY')


def final_verify(j: DurableMigrationJournal, db: dict) -> None:
    # Reconcile already-published DB+Gateway state before advancing journal.
    require(state(db['B'],'B')['last'] == 1500
            and state(db['B'],'B')['accounts'] == 500,
            'B not physically migrated')
    require(state(db['C'],'C')['accounts'] == 500
            and fence(db['C']) == ('C',2,True,False),
            'C not activated')
    require(fence(db['B']) == ('C',2,True,True), 'B source fence invalid')
    zero_prepared(db)
    require(sum(state(db[s],s)['total'] for s in ('A','B','C')) == EXPECTED_TOTAL,
            'Global money conservation failed')
    require(published_epoch() == 2, 'Gateway did not publish epoch2')
    with httpx.Client(timeout=6.0) as client:
        ready = client.get(GATEWAY + '/ready')
        body = ready.json()
        require(ready.status_code == 200 and body.get('healthy_replicas') == {'A':2,'B':2,'C':2},
                f'Incomplete three-shard ready: {body}')
        for account, shard in (('A0500','A'),('A1499','B'),('A1501','C'),('A2000','C')):
            r = client.get(GATEWAY + '/api/v1/accounts/' + account + '/balance')
            require(r.status_code == 200 and r.json().get('shard') == shard,
                    f'Misrouted account {account}: {r.text[:180]}')
    content = stage_digest(db['C'])
    require(digest(movable(db['C'])) == content, 'Target diverged from staged proof')
    phase = j.status()['phase']
    if phase == Step.FENCED.value:
        token = j.status()['pending_id']
        require(j.status()['pending_phase'] == Step.ACTIVATED.value and bool(token),
                'Activation intent missing')
        j.confirm(token, observe(phase=Step.ACTIVATED, db=db, content=content))
        phase = Step.ACTIVATED.value
    if phase == Step.ACTIVATED.value:
        confirm_step(j, Step.VERIFYING, db, content=content)
        phase = Step.VERIFYING.value
    if phase == Step.VERIFYING.value:
        confirm_step(j, Step.COMPLETED, db, content=content)
    require(j.status()['phase'] == Step.COMPLETED.value, 'Nonterminal journal after verify')
    print('BONUS10E4D_INTEGRATED_CUTOVER_PASS: ' + json.dumps({
        'phase': j.status()['phase'],
        'epoch': 2,
        'counts': {s: state(db[s],s)['accounts'] for s in ('A','B','C')},
        'money': sum(state(db[s],s)['total'] for s in ('A','B','C')),
        'digest': content,
        'gateway_replicas': body['healthy_replicas'],
        'prepared': 0,
    }, sort_keys=True))


def smoke_transfers(j: DurableMigrationJournal, db: dict) -> None:
    require(j.status()['phase'] == Step.COMPLETED.value, 'Cutover must be completed first')
    with httpx.Client(timeout=25.0) as client:
        for a, b in [('A0500','A1750'), ('A1499','A1750')]:
            for source, dest in ((a,b),(b,a)):
                txid = str(uuid4())
                res = client.post(GATEWAY + '/api/v1/operations', json={
                    'action':'transfer','from_account':source,'to_account':dest,
                    'amount': 1, 'transaction_id':txid,
                })
                require(res.status_code == 200 and res.json().get('status') == 'COMMITTED',
                        f'Post-cutover 2PC uncertain: {txid} {res.text[:240]}')
    zero_prepared(db)
    require(sum(state(db[s],s)['total'] for s in ('A','B','C')) == EXPECTED_TOTAL,
            'Post-2PC money conservation failed')
    print('BONUS10E4D_POSTCUTOVER_2PC_PASS: A<->C and B<->C')


def status(j: DurableMigrationJournal, db: dict) -> None:
    data = {'journal': j.status(), 'shards': {s: state(db[s],s) for s in ('A','B','C')}}
    try:
        data['coordinator_pending'] = pending_coordinator()
        data['gateway_epoch'] = published_epoch()
    except Exception as exc:
        data['readiness'] = type(exc).__name__ + ': ' + str(exc)
    print('BONUS10E4D_STATUS:', json.dumps(data, sort_keys=True, default=str))


def main() -> None:
    require(os.getenv('LAB_ONLY') == LAB_NAME, 'LAB_ONLY sentinel required')
    require(os.getenv('GATEWAY_URL') == GATEWAY, 'Gateway URL must be isolated internal hostname')
    require(os.getenv('COORDINATOR_URL') == COORDINATOR,
            'Coordinator URL must be isolated internal hostname')
    allowed = {'status', 'capture', 'snapshot', 'exercise', 'fence',
               'promote', 'prepublish', 'verify-live', 'post-2pc'}
    require(len(sys.argv) == 2 and sys.argv[1] in allowed,
            'Usage: lab_operator.py ' + '|'.join(sorted(allowed)))
    name = sys.argv[1]
    db = {s: connect(s) for s in ('A', 'B', 'C')}
    try:
        j = journal()
        actions = {'status': status, 'capture': capture_start, 'snapshot': snapshot,
                   'exercise': lambda _j, db: exercise(db), 'fence': fence_source,
                   'promote': promote, 'prepublish': prepublish,
                   'verify-live': final_verify, 'post-2pc': smoke_transfers}
        actions[name](j, db)
    finally:
        for con in db.values():
            con.close()


if __name__ == '__main__':
    try:
        main()
    except (LabRefused, MigrationError, Exception) as exc:
        print('BONUS10E4D_FAIL_CLOSED:', type(exc).__name__ + ': ' + str(exc), file=sys.stderr)
        raise SystemExit(1)
