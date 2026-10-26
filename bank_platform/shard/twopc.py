"""PostgreSQL-native 2PC participant: durable prepared xacts, never fake 2PC.

Both replicas of one shard share the same PostgreSQL catalog, so either can
finish a prepared GID after the other replica disappears. The ledger row is
committed *inside* the prepared transaction for idempotent COMMIT replay.
"""
from __future__ import annotations

import re
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
    if shard not in ('A', 'B'):
        raise ValueError('Unexpected shard')
    return 'bank_' + txid.replace('-', '') + '_' + shard.lower()


class Pg2pcParticipant:
    def __init__(self, storage: PostgresAccountStore):
        self.storage = storage

    @staticmethod
    def _exists(cursor, gid: str) -> bool:
        cursor.execute('SELECT 1 FROM pg_prepared_xacts WHERE gid = %s AND database = current_database()', (gid,))
        return cursor.fetchone() is not None

    def prepare(self, *, transaction_id: str, account: str, role: str, amount: int) -> str:
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
                if self._exists(cursor, gid):
                    return 'PREPARED'
                # Avoid waiting indefinitely behind a different prepared transfer.
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
                # Safe to interpolate only normalized UUIDv4 and fixed shard suffix.
                # PostgreSQL does not allow a bind parameter in PREPARE TRANSACTION.
                cursor.execute("PREPARE TRANSACTION '" + gid + "'")
        return 'PREPARED'

    def finalize(self, transaction_id: str, decision: str) -> str:
        txid = normalize_txid(transaction_id)
        if decision not in ('COMMIT', 'ABORT'):
            raise BankingError('INVALID_REQUEST', 'Expected COMMIT or ABORT', 400)
        gid = prepared_gid(txid, self.storage.shard_id)
        # COMMIT/ROLLBACK PREPARED cannot run inside another transaction block.
        with self.storage._connect() as conn:
            conn.autocommit = True
            with conn.cursor() as cursor:
                self.storage._verify_identity(cursor)
                cursor.execute('SELECT 1 FROM tpc_participant_commits WHERE transaction_id = %s', (txid,))
                committed = cursor.fetchone() is not None
                if committed:
                    if decision == 'ABORT':
                        raise BankingError('ALREADY_COMMITTED', 'Cannot abort a committed transaction', 409)
                    return 'COMMITTED'
                if self._exists(cursor, gid):
                    cursor.execute(("COMMIT PREPARED '" if decision == 'COMMIT' else "ROLLBACK PREPARED '") + gid + "'")
                elif decision == 'COMMIT':
                    # Missing prepared transaction is not proof that a COMMIT happened.
                    raise BankingError('PREPARED_TRANSACTION_MISSING', 'Unknown outcome: prepared transaction missing', 409)
                if decision == 'COMMIT':
                    cursor.execute('SELECT 1 FROM tpc_participant_commits WHERE transaction_id = %s', (txid,))
                    if cursor.fetchone() is None:
                        raise RuntimeError('COMMIT PREPARED did not write durable receipt')
                    return 'COMMITTED'
                return 'ABORTED'

    def inspect(self, transaction_id: str) -> str:
        txid = normalize_txid(transaction_id)
        gid = prepared_gid(txid, self.storage.shard_id)
        with self.storage._connect() as conn:
            with conn.cursor() as cursor:
                self.storage._verify_identity(cursor)
                cursor.execute('SELECT 1 FROM tpc_participant_commits WHERE transaction_id = %s', (txid,))
                if cursor.fetchone() is not None:
                    return 'COMMITTED'
                return 'PREPARED' if self._exists(cursor, gid) else 'ABSENT'
