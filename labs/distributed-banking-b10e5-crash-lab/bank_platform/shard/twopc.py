"""Crash-recoverable PostgreSQL participant with durable abort fencing.

A prepared transaction retains locks (including transaction-level advisory
locks) until COMMIT PREPARED / ROLLBACK PREPARED.  The abort fence is committed
outside the prepared transaction.  A late PREPARE is therefore unable to undo
a previously acknowledged ABORT, even after a timed-out request.
"""
from __future__ import annotations

import hashlib
from uuid import UUID

from bank_platform.common.validation import BankingError, MAX_MONEY
from bank_platform.shard.storage import PostgresAccountStore, ShardIdentityError


def normalize_txid(txid: str) -> str:
    try:
        value = UUID(txid)
    except (ValueError, TypeError, AttributeError):
        raise BankingError('INVALID_TRANSACTION_ID', 'transaction_id must be a UUID', 400) from None
    if value.version != 4:
        raise BankingError('INVALID_TRANSACTION_ID', 'transaction_id must be a UUIDv4', 400)
    return str(value)


def prepared_gid(txid: str, shard: str) -> str:
    txid = normalize_txid(txid)
    if shard not in ('A', 'B', 'C'):
        raise ValueError('Unexpected shard')
    return 'bank_' + txid.replace('-', '') + '_' + shard.lower()


def advisory_key(txid: str) -> int:
    # Stable across independent replicas and process restarts.  Collisions
    # only cause extra serialization, never reuse of the durable UUID key.
    digest = hashlib.sha256(normalize_txid(txid).encode('ascii')).digest()
    return int.from_bytes(digest[:8], 'big', signed=True)


class Pg2pcParticipant:
    def __init__(self, storage: PostgresAccountStore):
        self.storage = storage

    @staticmethod
    def _exists(cursor, gid: str) -> bool:
        cursor.execute('SELECT 1 FROM pg_prepared_xacts WHERE gid = %s AND database = current_database()', (gid,))
        return cursor.fetchone() is not None

    @staticmethod
    def _aborted(cursor, txid: str) -> bool:
        cursor.execute('SELECT 1 FROM tpc_participant_aborts WHERE transaction_id = %s', (txid,))
        return cursor.fetchone() is not None

    @staticmethod
    def _committed(cursor, txid: str) -> bool:
        cursor.execute('SELECT 1 FROM tpc_participant_commits WHERE transaction_id = %s', (txid,))
        return cursor.fetchone() is not None

    def prepare(self, *, transaction_id: str, account: str, role: str,
                amount: int, owner_epoch: int | None = None) -> str:
        txid = normalize_txid(transaction_id)
        if role not in ('debit', 'credit') or type(amount) is not int or not (1 <= amount <= MAX_MONEY):
            raise BankingError('INVALID_REQUEST', 'Invalid role or amount', 400)
        if not self.storage.owns(account):
            raise ShardIdentityError('Wrong account owner')
        gid = prepared_gid(txid, self.storage.shard_id)
        with self.storage._connect() as conn:
            with conn.cursor() as cursor:
                self.storage._verify_identity(cursor)
                cursor.execute('SELECT account_id, role, amount FROM tpc_participant_commits WHERE transaction_id = %s', (txid,))
                receipt = cursor.fetchone()
                if receipt is not None:
                    if (receipt[0], receipt[1], int(receipt[2])) != (account, role, amount):
                        raise BankingError('TRANSACTION_ID_CONFLICT', 'Different payload for used transaction_id', 409)
                    return 'COMMITTED'
                if self._aborted(cursor, txid):
                    raise BankingError('TRANSACTION_ABORTED', 'A durable ABORT already fenced this ID', 409)
                if self._exists(cursor, gid):
                    return 'PREPARED'
                # Non-blocking transaction lock: if another prepare is in
                # flight or already prepared, return uncertainty instead of
                # creating a second prepared transaction.
                cursor.execute('SELECT pg_try_advisory_xact_lock(%s)', (advisory_key(txid),))
                if not cursor.fetchone()[0]:
                    if self._exists(cursor, gid):
                        return 'PREPARED'
                    raise BankingError('PREPARE_IN_PROGRESS', 'The same transaction is preparing', 503)
                # Check again *after* acquiring the lock: ABORT may have
                # installed a fence while we waited to enter this transaction.
                if self._aborted(cursor, txid):
                    raise BankingError('TRANSACTION_ABORTED', 'A durable ABORT already fenced this ID', 409)
                if self._exists(cursor, gid):
                    return 'PREPARED'
                # Bound in the PREPARE transaction itself: COMMIT PREPARED later
                # needs no epoch, preserving recovery of historical decisions.
                self.storage._bind_owner_epoch(cursor, owner_epoch)
                cursor.execute("SET LOCAL lock_timeout = '5s'")
                cursor.execute('SELECT balance FROM accounts WHERE account_id = %s FOR UPDATE', (account,))
                row = cursor.fetchone()
                if row is None:
                    raise BankingError('ACCOUNT_NOT_FOUND', 'Account does not exist', 404)
                balance = int(row[0])
                if role == 'debit' and balance < amount:
                    raise BankingError('INSUFFICIENT_FUNDS', 'Insufficient funds', 409)
                if role == 'credit' and balance > MAX_MONEY - amount:
                    raise BankingError('BALANCE_OVERFLOW', 'Balance overflow', 409)
                new_balance = balance - amount if role == 'debit' else balance + amount
                cursor.execute('UPDATE accounts SET balance = %s WHERE account_id = %s', (new_balance, account))
                cursor.execute('INSERT INTO tpc_participant_commits (transaction_id, account_id, role, amount) VALUES (%s, %s, %s, %s)', (txid, account, role, amount))
                # Only fixed prefix/suffix and normalized UUIDv4 enter GID.
                cursor.execute("PREPARE TRANSACTION '" + gid + "'")
        return 'PREPARED'

    def finalize(self, transaction_id: str, decision: str) -> str:
        txid = normalize_txid(transaction_id)
        if decision not in ('COMMIT', 'ABORT'):
            raise BankingError('INVALID_REQUEST', 'Expected COMMIT or ABORT', 400)
        gid = prepared_gid(txid, self.storage.shard_id)
        with self.storage._connect() as conn:
            conn.autocommit = True  # COMMIT PREPARED cannot execute inside BEGIN
            with conn.cursor() as cursor:
                self.storage._verify_identity(cursor)
                if self._committed(cursor, txid):
                    if decision == 'ABORT':
                        raise BankingError('ALREADY_COMMITTED', 'Cannot abort a committed transaction', 409)
                    return 'COMMITTED'
                if decision == 'COMMIT':
                    if self._aborted(cursor, txid):
                        raise BankingError('ALREADY_ABORTED', 'Cannot commit a fenced transaction', 409)
                    if self._exists(cursor, gid):
                        cursor.execute("COMMIT PREPARED '" + gid + "'")
                    else:
                        raise BankingError('PREPARED_TRANSACTION_MISSING', 'Prepared transaction missing', 409)
                    if not self._committed(cursor, txid):
                        raise RuntimeError('COMMIT PREPARED did not write durable receipt')
                    return 'COMMITTED'
                # ABORT.  If PREPARED is visible, write a durable fence first;
                # after rollback no concurrent late prepare may pass its check.
                if self._exists(cursor, gid):
                    cursor.execute('INSERT INTO tpc_participant_aborts (transaction_id) VALUES (%s) ON CONFLICT DO NOTHING', (txid,))
                    cursor.execute("ROLLBACK PREPARED '" + gid + "'")
                    return 'ABORTED'
                # If an in-flight PREPARE holds the advisory lock, do NOT
                # acknowledge ABORT. Retry after it resolves; if available,
                # acquire the lock and atomically commit the abort fence.
                cursor.execute('BEGIN')
                try:
                    cursor.execute('SELECT pg_try_advisory_xact_lock(%s)', (advisory_key(txid),))
                    if not cursor.fetchone()[0]:
                        cursor.execute('ROLLBACK')
                        return 'PENDING'
                    if self._committed(cursor, txid):
                        cursor.execute('ROLLBACK')
                        raise BankingError('ALREADY_COMMITTED', 'Cannot abort a committed transaction', 409)
                    if self._exists(cursor, gid):
                        # Defensive: should not happen while we hold the
                        # advisory transaction lock.
                        cursor.execute('ROLLBACK')
                        return 'PENDING'
                    cursor.execute('INSERT INTO tpc_participant_aborts (transaction_id) VALUES (%s) ON CONFLICT DO NOTHING', (txid,))
                    cursor.execute('COMMIT')
                except Exception:
                    if not conn.closed:
                        cursor.execute('ROLLBACK')
                    raise
                return 'ABORTED'

    def inspect(self, transaction_id: str) -> str:
        txid = normalize_txid(transaction_id)
        gid = prepared_gid(txid, self.storage.shard_id)
        with self.storage._connect() as conn:
            with conn.cursor() as cursor:
                self.storage._verify_identity(cursor)
                if self._committed(cursor, txid):
                    return 'COMMITTED'
                # An ABORT fence may precede ROLLBACK PREPARED; it does not
                # prove the prepared lock has been released yet.
                if self._exists(cursor, gid):
                    return 'PREPARED'
                if self._aborted(cursor, txid):
                    return 'ABORTED'
                cursor.execute('SELECT pg_try_advisory_xact_lock(%s)', (advisory_key(txid),))
                return 'ABSENT' if cursor.fetchone()[0] else 'PREPARING'
