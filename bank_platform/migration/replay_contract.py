"""Pure revision-based CDC/replay contract for the moved account range.

Do not use a sequence event_id as a committed-order high-water mark:
PostgreSQL sequences are allocated before COMMIT and can become visible out of
order. Revisions on an individually locked account provide gap detection.
This is an offline contract test/reference; the real target needs transactional
UPSERT and durable receipts in PostgreSQL (a subsequent integration gate).
"""
from __future__ import annotations
from dataclasses import dataclass
from hashlib import sha256

from bank_platform.common.shard_map import account_number
from bank_platform.common.validation import MAX_MONEY


class ReplayError(RuntimeError):
    pass


@dataclass(frozen=True)
class AccountRevision:
    account_id: str
    balance: int
    revision: int

    def validate(self) -> None:
        n = account_number(self.account_id)
        if not 1501 <= n <= 2000:
            raise ReplayError('Only A1501..A2000 may migrate')
        if type(self.balance) is not int or not 0 <= self.balance <= MAX_MONEY:
            raise ReplayError('Balance must be a nonnegative integer')
        if type(self.revision) is not int or self.revision < 0:
            raise ReplayError('Revision must be a nonnegative integer')


class RevisionReplay:
    def __init__(self):
        self._rows: dict[str, AccountRevision] = {}
        self._receipt_hash: dict[tuple[str,int], str] = {}

    @staticmethod
    def _fingerprint(row: AccountRevision) -> str:
        return sha256(f'{row.account_id}|{row.revision}|{row.balance}'.encode()).hexdigest()

    def snapshot(self, row: AccountRevision) -> bool:
        row.validate()
        old = self._rows.get(row.account_id)
        if old is not None:
            if old.revision > row.revision:
                return False  # Never overwrite already-replayed new data.
            if old.revision == row.revision:
                if old.balance != row.balance:
                    raise ReplayError('Same revision with conflicting snapshot')
                return False
        self._rows[row.account_id] = row
        self._receipt_hash[(row.account_id,row.revision)] = self._fingerprint(row)
        return True

    def apply(self, change: AccountRevision) -> bool:
        change.validate()
        old = self._rows.get(change.account_id)
        if old is None:
            raise ReplayError('Snapshot row missing')
        key = (change.account_id, change.revision)
        if change.revision <= old.revision:
            receipt = self._receipt_hash.get(key)
            if receipt is None or receipt != self._fingerprint(change):
                raise ReplayError('Unverifiable old or conflicting replay event')
            return False
        if change.revision != old.revision + 1:
            raise ReplayError('CDC revision gap; refuse to skip an unseen write')
        self._rows[change.account_id] = change
        self._receipt_hash[key] = self._fingerprint(change)
        return True

    def verify(self, source_rows: list[AccountRevision]) -> str:
        expected: dict[str, AccountRevision] = {}
        for row in source_rows:
            row.validate()
            if row.account_id in expected:
                raise ReplayError('Duplicate source account')
            expected[row.account_id] = row
        if len(expected) != 500 or set(expected) != set(self._rows):
            raise ReplayError('Moving account set is incomplete')
        if expected != self._rows:
            raise ReplayError('Source and target differ in balance/revision')
        raw = ''.join(f'{r.account_id}:{r.balance}:{r.revision}\n'
                      for r in sorted(expected.values(),key=lambda r:r.account_id))
        return sha256(raw.encode('utf-8')).hexdigest()

    def rows(self) -> list[AccountRevision]:
        return [self._rows[k] for k in sorted(self._rows)]
