"""Fail-closed, per-account revision rules for the B -> C staging mirror.

These immutable values are deliberately not tied to a global CDC sequence:
PostgreSQL sequences are not commit-order clocks.
"""
from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256

from bank_platform.migration.replay_contract import AccountRevision


class StageConflict(RuntimeError):
    pass


@dataclass(frozen=True)
class StagedRow:
    account_id: str
    balance: int
    revision: int
    snapshot_revision: int
    snapshot_balance: int

    def validate(self) -> None:
        AccountRevision(self.account_id, self.balance, self.revision).validate()
        if type(self.snapshot_revision) is not int or not 0 <= self.snapshot_revision <= self.revision:
            raise StageConflict('Invalid snapshot revision')
        AccountRevision(self.account_id, self.snapshot_balance, self.snapshot_revision).validate()


@dataclass(frozen=True)
class ReplayReceipt:
    account_id: str
    balance: int
    revision: int

    def validate(self) -> None:
        AccountRevision(self.account_id, self.balance, self.revision).validate()
        if self.revision == 0:
            raise StageConflict('A CDC receipt requires revision > 0')


def accept_snapshot(existing: StagedRow | None, incoming: AccountRevision) -> tuple[StagedRow, bool]:
    """First snapshot inserts a row; resume must never lower a staged revision.

    Once a snapshot base is recorded, a different base for the same account is
    not accepted automatically: that would make a partial failed run ambiguous.
    """
    incoming.validate()
    if existing is None:
        return StagedRow(incoming.account_id, incoming.balance,
                         incoming.revision, incoming.revision, incoming.balance), True
    existing.validate()
    if existing.account_id != incoming.account_id:
        raise StageConflict('Different account in snapshot replay')
    if (existing.snapshot_revision, existing.snapshot_balance) != (incoming.revision, incoming.balance):
        raise StageConflict('Snapshot checkpoint or original balance changed')
    if existing.revision == incoming.revision and existing.balance != incoming.balance:
        raise StageConflict('Conflicting snapshot contents')
    if existing.revision < incoming.revision:
        raise StageConflict('Staging row is behind its own checkpoint')
    return existing, False


def accept_change(existing: StagedRow | None, receipt: ReplayReceipt | None,
                  incoming: AccountRevision) -> tuple[StagedRow, ReplayReceipt | None, bool]:
    """Apply a single account revision once, never skip a gap or invent a rollback."""
    incoming.validate()
    if incoming.revision == 0:
        raise StageConflict('CDC revision must be positive')
    if existing is None:
        raise StageConflict('Snapshot missing: cannot apply CDC')
    existing.validate()
    if existing.account_id != incoming.account_id:
        raise StageConflict('CDC account does not match locked staging row')
    if receipt is not None:
        receipt.validate()
        if (receipt.account_id, receipt.revision, receipt.balance) != (
                incoming.account_id, incoming.revision, incoming.balance):
            raise StageConflict('Conflicting previously applied CDC receipt')
        if existing.revision < incoming.revision:
            raise StageConflict('Receipt exists without its staged state')
        return existing, receipt, False
    if incoming.revision <= existing.snapshot_revision:
        return existing, None, False  # Included in the proven immutable snapshot.
    if incoming.revision <= existing.revision:
        raise StageConflict('Missing receipt for an already advanced revision')
    if incoming.revision != existing.revision + 1:
        raise StageConflict('Per-account CDC revision gap')
    return (StagedRow(existing.account_id, incoming.balance, incoming.revision,
                      existing.snapshot_revision, existing.snapshot_balance),
            ReplayReceipt(incoming.account_id, incoming.balance, incoming.revision), True)


def digest(rows: list[AccountRevision]) -> str:
    """Content hash: all and only A1501..A2000, with balance and revision."""
    expected = {f'A{number:04d}' for number in range(1501, 2001)}
    seen: set[str] = set()
    parts = []
    for row in rows:
        row.validate()
        if row.account_id in seen:
            raise StageConflict('Duplicate account')
        seen.add(row.account_id)
        parts.append(f'{row.account_id}:{row.balance}:{row.revision}\n')
    if seen != expected:
        raise StageConflict('Incomplete 500-account moving range')
    return sha256(''.join(sorted(parts)).encode('utf-8')).hexdigest()
