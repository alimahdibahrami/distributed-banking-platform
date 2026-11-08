"""Durable, *non-executing* online-resharding control-plane.

This package records intent and observed proofs. It intentionally does not
connect to PostgreSQL, modify a shard map, or perform a cutover. Those side
effects need a separate, reviewed database-backed runner in the next gate.

The point is that a process crash between DB work and acknowledgement never
silently advances state. On restart, the pending intent remains visible and
must be reconciled with the authoritative database observations.
"""
from __future__ import annotations

import hashlib
import re
import json
import sqlite3
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from enum import Enum
from pathlib import Path
from uuid import UUID, uuid4


class MigrationError(RuntimeError):
    pass


class Step(str, Enum):
    PLANNED = 'PLANNED'
    SNAPSHOTTING = 'SNAPSHOTTING'
    CATCHING_UP = 'CATCHING_UP'
    FENCED = 'FENCED'
    ACTIVATED = 'ACTIVATED'
    VERIFYING = 'VERIFYING'
    COMPLETED = 'COMPLETED'


NEXT = {
    Step.PLANNED: Step.SNAPSHOTTING,
    Step.SNAPSHOTTING: Step.CATCHING_UP,
    Step.CATCHING_UP: Step.FENCED,
    Step.FENCED: Step.ACTIVATED,
    Step.ACTIVATED: Step.VERIFYING,
    Step.VERIFYING: Step.COMPLETED,
}


@dataclass(frozen=True)
class MigrationPlan:
    migration_id: str
    source: str = 'B'
    target: str = 'C'
    first: str = 'A1501'
    last: str = 'A2000'
    account_count: int = 500
    old_epoch: int = 1
    new_epoch: int = 2

    def validate(self) -> None:
        try:
            u = UUID(self.migration_id)
        except (ValueError, TypeError, AttributeError) as exc:
            raise MigrationError('Invalid migration UUID') from exc
        if u.version != 4:
            raise MigrationError('Migration ID must be UUID v4')
        if (self.source, self.target, self.first, self.last, self.account_count) != (
            'B', 'C', 'A1501', 'A2000', 500
        ):
            raise MigrationError('Only the assignment B-to-C range is supported')
        if (type(self.old_epoch) is not int or type(self.new_epoch) is not int
                or self.old_epoch < 1 or self.new_epoch <= self.old_epoch):
            raise MigrationError('Epoch must increase monotonically')


@dataclass(frozen=True)
class Observation:
    """Readback evidence collected by a future trusted DB-bound observer.

    This is not a security attestation; accepting Observation objects alone
    never authorizes an actual database side effect.
    """
    capture_enabled: bool = False
    snapshot_persisted: bool = False
    target_standby: bool = False
    source_fenced: bool = False
    target_active: bool = False
    map_published: bool = False
    gateway_epoch: int = 1
    source_prepared: int = 0
    target_prepared: int = 0
    coordinator_pending: int = 0
    lagging_accounts: int = 0
    moved_accounts: int = 0
    source_digest: str = ''
    target_digest: str = ''
    prior_owner_rows: int = 500
    source_epoch: int = 1
    target_epoch: int = 2

    def validate(self, plan: MigrationPlan, phase: Step) -> None:
        for name in ('capture_enabled', 'snapshot_persisted', 'target_standby', 'source_fenced',
                     'target_active', 'map_published'):
            if type(getattr(self, name)) is not bool:
                raise MigrationError(f'{name} must be a bool')
        for name in ('gateway_epoch', 'source_prepared', 'target_prepared',
                     'coordinator_pending', 'lagging_accounts', 'moved_accounts',
                     'prior_owner_rows', 'source_epoch', 'target_epoch'):
            value = getattr(self, name)
            if type(value) is not int or value < 0:
                raise MigrationError(f'{name} must be a nonnegative integer')
        if self.source_epoch != plan.old_epoch or self.target_epoch != plan.new_epoch:
            raise MigrationError('Observed ownership epochs do not match immutable plan')
        if phase in (Step.SNAPSHOTTING, Step.CATCHING_UP, Step.FENCED):
            if self.target_active or self.map_published or self.gateway_epoch != plan.old_epoch:
                raise MigrationError('Never publish a new owner before the fenced cutover')
        if phase == Step.SNAPSHOTTING:
            if not (self.capture_enabled and self.target_standby):
                raise MigrationError('CDC must be durably enabled before snapshot')
        if phase == Step.CATCHING_UP:
            if not (self.capture_enabled and self.snapshot_persisted and self.target_standby):
                raise MigrationError('Snapshot and CDC must be durable before catch-up')
        if phase in (Step.FENCED, Step.ACTIVATED, Step.VERIFYING, Step.COMPLETED):
            if not self.source_fenced:
                raise MigrationError('Source must be durably fenced before activation')
            if self.source_prepared or self.target_prepared or self.coordinator_pending:
                raise MigrationError('2PC transactions must be drained before cutover')
            if self.lagging_accounts != 0:
                raise MigrationError('CDC revision gaps prevent cutover')
            if self.moved_accounts != plan.account_count:
                raise MigrationError('Target account count mismatch')
            if (not re.fullmatch(r'[0-9a-f]{64}', self.source_digest)
                    or not re.fullmatch(r'[0-9a-f]{64}', self.target_digest)
                    or self.source_digest != self.target_digest):
                raise MigrationError('Source and target balance/revision digests differ')
        if phase == Step.FENCED and not self.target_standby:
            raise MigrationError('Target must remain standby while old owner fenced')
        if phase in (Step.ACTIVATED, Step.VERIFYING, Step.COMPLETED):
            if not (self.target_active and not self.target_standby
                    and self.map_published and self.gateway_epoch == plan.new_epoch):
                raise MigrationError('Activation and published gateway epoch must agree')
        if phase == Step.COMPLETED and self.prior_owner_rows != 0:
            raise MigrationError('Old owner must no longer retain moving rows')


def _canonical(data: object) -> str:
    return json.dumps(data, sort_keys=True, separators=(',', ':'), ensure_ascii=False)


class DurableMigrationJournal:
    """A single-writer SQLite WAL-free journal with FULL synchronous commits.

    Use a local persistent disk, not /tmp and not an ephemeral container layer.
    The journal is an operator control aid; it cannot atomically commit with
    two remote PostgreSQL databases, so reconciliation is always required.
    """
    def __init__(self, path: str | Path):
        self.path = Path(path)

    @contextmanager
    def _transaction(self):
        if not self.path.parent.exists():
            self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(str(self.path), timeout=15, isolation_level=None)
        try:
            connection.row_factory = sqlite3.Row
            connection.execute('PRAGMA synchronous=FULL')
            connection.execute('PRAGMA foreign_keys=ON')
            connection.execute('BEGIN IMMEDIATE')
            yield connection
            connection.execute('COMMIT')
        except BaseException:
            connection.execute('ROLLBACK')
            raise
        finally:
            connection.close()

    def initialize(self, plan: MigrationPlan) -> None:
        plan.validate()
        with self._transaction() as con:
            con.execute('''CREATE TABLE IF NOT EXISTS migration_control (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                migration_id TEXT NOT NULL, plan_json TEXT NOT NULL,
                phase TEXT NOT NULL,
                pending_id TEXT, pending_phase TEXT,
                transition_count INTEGER NOT NULL DEFAULT 0
            )''')
            con.execute('''CREATE TABLE IF NOT EXISTS migration_evidence (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                migration_id TEXT NOT NULL, phase TEXT NOT NULL,
                sha256 TEXT NOT NULL, evidence_json TEXT NOT NULL,
                completed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )''')
            existing = con.execute('SELECT plan_json FROM migration_control WHERE singleton=1').fetchone()
            document = _canonical(asdict(plan))
            if existing and existing['plan_json'] != document:
                raise MigrationError('Immutable migration plan mismatch')
            if not existing:
                con.execute('INSERT INTO migration_control (singleton,migration_id,plan_json,phase) '
                            'VALUES (1,?,?,?)', (plan.migration_id, document, Step.PLANNED.value))

    def status(self) -> dict:
        with self._transaction() as con:
            row = con.execute('SELECT * FROM migration_control WHERE singleton=1').fetchone()
            if row is None:
                raise MigrationError('Journal not initialized')
            return dict(row)

    def plan(self) -> MigrationPlan:
        return MigrationPlan(**json.loads(self.status()['plan_json']))

    def begin(self, next_phase: Step) -> str:
        if not isinstance(next_phase, Step):
            raise MigrationError('Unknown transition')
        with self._transaction() as con:
            row = con.execute('SELECT * FROM migration_control WHERE singleton=1').fetchone()
            if row is None:
                raise MigrationError('Journal not initialized')
            if row['pending_id']:
                if row['pending_phase'] == next_phase.value:
                    return row['pending_id']  # Resume, do not invent another operation.
                raise MigrationError('Previous operation unresolved; reconcile before advancing')
            if NEXT.get(Step(row['phase'])) != next_phase:
                raise MigrationError('Transitions must follow the migration state machine')
            token = str(uuid4())
            con.execute('UPDATE migration_control SET pending_id=?, pending_phase=? WHERE singleton=1',
                        (token, next_phase.value))
            return token

    def confirm(self, token: str, observation: Observation) -> dict:
        if not isinstance(observation, Observation):
            raise MigrationError('An explicit observation is required')
        with self._transaction() as con:
            row = con.execute('SELECT * FROM migration_control WHERE singleton=1').fetchone()
            if row is None or row['pending_id'] != token:
                raise MigrationError('Unknown, stale or already confirmed operation token')
            next_phase = Step(row['pending_phase'])
            plan = MigrationPlan(**json.loads(row['plan_json']))
            observation.validate(plan, next_phase)
            evidence = _canonical(asdict(observation))
            proof_sha = hashlib.sha256(evidence.encode('utf-8')).hexdigest()
            con.execute('INSERT INTO migration_evidence (migration_id,phase,sha256,evidence_json) '
                        'VALUES (?,?,?,?)', (plan.migration_id, next_phase.value, proof_sha, evidence))
            con.execute('''UPDATE migration_control SET phase=?,pending_id=NULL,pending_phase=NULL,
                           transition_count=transition_count+1 WHERE singleton=1''', (next_phase.value,))
            return {'phase': next_phase.value, 'evidence_sha256': proof_sha,
                    'transition_count': row['transition_count'] + 1}

    def history(self) -> list[dict]:
        with self._transaction() as con:
            return [dict(r) for r in con.execute(
                'SELECT seq,migration_id,phase,sha256,completed_at FROM migration_evidence ORDER BY seq')]

    def assert_no_unreconciled_intent(self) -> None:
        state = self.status()
        if state['pending_id']:
            raise MigrationError('Unreconciled intent: inspect authoritative DB states first')


def new_plan() -> MigrationPlan:
    return MigrationPlan(migration_id=str(uuid4()))
