"""PostgreSQL-backed STAGING mirror, not a live cutover controller.

Source B: read-only snapshot/CDC; Target C: write only migration staging
and receipts, NEVER accounts, metadata, fence, shard map or 2PC journal.
The caller supplies dedicated DB connections and is responsible for operator
review of access; these helpers have no CLI to activate a production owner.
"""
from __future__ import annotations

from bank_platform.migration.replay_contract import AccountRevision
from bank_platform.migration.staging_contract import (
    StageConflict, StagedRow, ReplayReceipt, accept_change, accept_snapshot, digest,
)


class StagingDatabaseError(RuntimeError):
    pass


class PostgreSQLStaging:
    def __init__(self, source, target):
        self.source, self.target = source, target

    def _read_source(self, *, fenced: bool = False) -> list[AccountRevision]:
        """Take a single repeatable-read snapshot, never relying on event_id."""
        with self.source.transaction():
            self.source.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY')
            gate = self.source.execute('''SELECT owner_shard,owner_epoch,fence_enabled,capture_enabled
               FROM bank_migration_fence WHERE singleton=1''').fetchone()
            if gate is None or gate[3] is not True:
                raise StagingDatabaseError('B must capture every update before taking a snapshot')
            if fenced:
                if (str(gate[0]).strip(), int(gate[1]), gate[2]) != ('C', 2, True):
                    raise StagingDatabaseError('B is not durably fenced to C epoch 2')
                num_prepared = self.source.execute('''SELECT count(*) FROM pg_prepared_xacts
                    WHERE database=current_database()''').fetchone()[0]
                if num_prepared:
                    raise StagingDatabaseError('Prepared 2PC transactions remain on B')
            else:
                if (str(gate[0]).strip(), int(gate[1]), gate[2]) != ('B', 1, False):
                    raise StagingDatabaseError('Unexpected B ownership before fence')
            fetched = self.source.execute('''SELECT account_id,balance,migration_revision
                FROM accounts WHERE account_id BETWEEN 'A1501' AND 'A2000' ORDER BY account_id''').fetchall()
            rows = [AccountRevision(str(a), int(b), int(r)) for a, b, r in fetched]
            digest(rows)  # 500 immutable account IDs, no gaps or extras.
            return rows

    def snapshot(self) -> dict:
        rows = self._read_source()
        with self.target.transaction():
            self._check_target_standby()
            for r in rows:
                old = self.target.execute('''SELECT account_id,balance,account_revision,snapshot_revision,snapshot_balance
                  FROM bank_migration_stage_accounts WHERE account_id=%s FOR UPDATE''',
                  (r.account_id,)).fetchone()
                old = StagedRow(str(old[0]), int(old[1]), int(old[2]), int(old[3]), int(old[4])) if old else None
                new, changed = accept_snapshot(old, r)
                if changed:
                    self.target.execute('''INSERT INTO bank_migration_stage_accounts
                       (account_id,balance,account_revision,snapshot_revision,snapshot_balance) VALUES (%s,%s,%s,%s,%s)''',
                       (new.account_id,new.balance,new.revision,new.snapshot_revision,new.snapshot_balance))
        return {'accounts': len(rows), 'snapshot_digest': digest(rows)}

    def _check_target_standby(self) -> None:
        gate = self.target.execute('''SELECT fence_enabled,capture_enabled FROM bank_migration_fence
            WHERE singleton=1''').fetchone()
        if gate is None or gate[0] is not False or gate[1] is not False:
            raise StagingDatabaseError('C staging requires an inactive, non-capturing target')

    def changes(self) -> list[AccountRevision]:
        with self.source.transaction():
            self.source.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY')
            gate = self.source.execute('''SELECT capture_enabled FROM bank_migration_fence
                WHERE singleton=1''').fetchone()
            if gate is None or gate[0] is not True:
                raise StagingDatabaseError('CDC capture is not active on B')
            fetched = self.source.execute('''SELECT account_id,resulting_balance,account_revision
                FROM bank_migration_cdc WHERE account_id BETWEEN 'A1501' AND 'A2000'
                ORDER BY account_id,account_revision''').fetchall()
        return [AccountRevision(str(a), int(b), int(r)) for a, b, r in fetched]

    def replay(self) -> dict:
        events = self.changes()
        applied = 0
        with self.target.transaction():
            self._check_target_standby()
            for r in events:
                old = self.target.execute('''SELECT account_id,balance,account_revision,snapshot_revision,snapshot_balance
                    FROM bank_migration_stage_accounts WHERE account_id=%s FOR UPDATE''',
                    (r.account_id,)).fetchone()
                old = StagedRow(str(old[0]), int(old[1]), int(old[2]), int(old[3]), int(old[4])) if old else None
                receipt = self.target.execute('''SELECT account_id,balance,account_revision
                    FROM bank_migration_stage_receipts WHERE account_id=%s AND account_revision=%s''',
                    (r.account_id, r.revision)).fetchone()
                receipt = ReplayReceipt(str(receipt[0]), int(receipt[1]), int(receipt[2])) if receipt else None
                new, new_receipt, changed = accept_change(old, receipt, r)
                if changed:
                    self.target.execute('''UPDATE bank_migration_stage_accounts
                       SET balance=%s,account_revision=%s WHERE account_id=%s''',
                       (new.balance,new.revision,new.account_id))
                    self.target.execute('''INSERT INTO bank_migration_stage_receipts
                       (account_id,account_revision,balance) VALUES (%s,%s,%s)''',
                       (new_receipt.account_id,new_receipt.revision,new_receipt.balance))
                    applied += 1
        return {'events_seen': len(events), 'newly_applied': applied}

    def fenced_convergence(self) -> dict:
        """Data consistency only, NOT permission to publish routing or cutover."""
        source_rows = self._read_source(fenced=True)
        with self.target.transaction():
            self._check_target_standby()
            num_prepared = self.target.execute('''SELECT count(*) FROM pg_prepared_xacts
                WHERE database=current_database()''').fetchone()[0]
            if num_prepared:
                raise StagingDatabaseError('Prepared 2PC transactions remain on C')
            fetched = self.target.execute('''SELECT account_id,balance,account_revision
                FROM bank_migration_stage_accounts ORDER BY account_id''').fetchall()
        target_rows = [AccountRevision(str(a), int(b), int(r)) for a, b, r in fetched]
        s = digest(source_rows)
        t = digest(target_rows)
        if s != t:
            raise StageConflict('Source and C staging differ; replay or resolve gaps')
        return {'moving_accounts': 500, 'data_digest': s,
                'data_converged': True, 'cutover_authorized': False}
