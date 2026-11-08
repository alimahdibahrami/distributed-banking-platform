"""Authoritative PostgreSQL storage shared by replicas within a shard.

Transactions and row locks run in PostgreSQL, never in per-replica memory.
"""
from __future__ import annotations

import os
from dataclasses import dataclass

from bank_platform.common.shard_map import account_number
from bank_platform.common.validation import BankingError, MAX_MONEY
from bank_platform.migration.epoch import stale_epoch_error


@dataclass(frozen=True)
class DbConfig:
    host: str
    dbname: str
    user: str
    password: str
    port: int = 5432

    @classmethod
    def from_env(cls) -> "DbConfig":
        return cls(
            host=os.getenv("BANK_DB_HOST", ""),
            dbname=os.getenv("BANK_DB_NAME", ""),
            user=os.getenv("BANK_DB_USER", ""),
            password=os.getenv("BANK_DB_PASSWORD", ""),
            port=int(os.getenv("BANK_DB_PORT", "5432")),
        )


class ShardIdentityError(RuntimeError):
    """The configured database does not belong to the expected shard."""


class PostgresAccountStore:
    def __init__(self, shard_id: str, config: DbConfig | None = None):
        if shard_id not in {"A", "B", "C"}:
            raise ValueError("Unknown shard")
        self.shard_id = shard_id
        self.config = config or DbConfig.from_env()
        defaults = {"A": (1, 1000), "B": (1001, 2000), "C": (1501, 2000)}
        first, last = defaults[shard_id]
        # B's new 1001..1500 identity is only valid after a controlled DB
        # migration; an environment override alone never alters DB ownership.
        self.first = int(os.getenv("BANK_ACCOUNT_FIRST", str(first)))
        self.last = int(os.getenv("BANK_ACCOUNT_LAST", str(last)))
        if (self.first, self.last) not in {(1, 1000), (1001, 2000), (1001, 1500), (1501, 2000)}:
            raise ValueError("Unexpected shard range")
        if shard_id == "A" and (self.first, self.last) != (1, 1000):
            raise ValueError("Invalid shard A range")
        if shard_id == "B" and (self.first, self.last) not in {(1001, 2000), (1001, 1500)}:
            raise ValueError("Invalid shard B range")
        if shard_id == "C" and (self.first, self.last) != (1501, 2000):
            raise ValueError("Invalid shard C range")

    def owns(self, account_id: str) -> bool:
        number = account_number(account_id)
        return self.first <= number <= self.last

    def _connect(self):
        # Import is deferred to permit fast unit tests of the scaffold even
        # when Docker-only runtime requirements are not installed locally.
        import psycopg

        c = self.config
        if not all((c.host, c.dbname, c.user, c.password)):
            raise RuntimeError("Database environment is not configured")
        return psycopg.connect(
            host=c.host, port=c.port, dbname=c.dbname, user=c.user,
            password=c.password, connect_timeout=3,
        )

    def _verify_identity(self, cursor) -> None:
        cursor.execute("SELECT shard_id, first_account, last_account, schema_version "
                       "FROM shard_metadata WHERE singleton = 1")
        row = cursor.fetchone()
        if row is None or (row[0], row[1], row[2], row[3]) != (
            self.shard_id, self.first, self.last, 1
        ):
            raise ShardIdentityError(f"Database identity is not shard {self.shard_id}")

    def _bind_owner_epoch(self, cursor, owner_epoch: int | None) -> None:
        """Set transaction-local PostgreSQL owner context before any write.

        The database trigger is authoritative. This method does not decide
        account ownership or permit any bypass of that trigger.
        """
        if owner_epoch is None:
            return
        if type(owner_epoch) is not int or owner_epoch < 1:
            raise BankingError("INVALID_OWNER_EPOCH", "Positive integer required", 400)
        cursor.execute("SELECT set_config('bank.owner_shard', %s, true)", (self.shard_id,))
        cursor.execute("SELECT set_config('bank.owner_epoch', %s, true)", (str(owner_epoch),))

    def ready(self) -> bool:
        with self._connect() as connection:
            with connection.cursor() as cursor:
                self._verify_identity(cursor)
                cursor.execute("SELECT count(*) FROM accounts")
                count = cursor.fetchone()[0]
                if count != self.last - self.first + 1:
                    raise ShardIdentityError("Account fixture is incomplete")
        return True

    def read_account(self, account_id: str) -> int | None:
        if not self.owns(account_id):
            raise ShardIdentityError(f"Account does not belong to shard {self.shard_id}")
        with self._connect() as connection:
            with connection.cursor() as cursor:
                self._verify_identity(cursor)
                cursor.execute("SELECT balance FROM accounts WHERE account_id = %s", (account_id,))
                row = cursor.fetchone()
                return int(row[0]) if row is not None else None


    def mutate(self, action: str, account_id: str, amount: int, *, owner_epoch: int | None = None) -> int:
        """One local ACID transaction, SERIALIZE same-account writes with row lock."""
        if action not in ("deposit", "withdraw"):
            raise ValueError("Unsupported mutation")
        if not self.owns(account_id):
            raise ShardIdentityError("Account belongs to another shard")
        if type(amount) is not int or not 1 <= amount <= MAX_MONEY:
            raise ValueError("Invalid amount")
        with self._connect() as connection:
            with connection.cursor() as cursor:
                self._verify_identity(cursor)
                self._bind_owner_epoch(cursor, owner_epoch)
                cursor.execute("SELECT balance FROM accounts WHERE account_id = %s FOR UPDATE", (account_id,))
                row = cursor.fetchone()
                if row is None:
                    raise BankingError("ACCOUNT_NOT_FOUND", "Account does not exist", 404)
                old = int(row[0])
                if action == "withdraw" and amount > old:
                    raise BankingError("INSUFFICIENT_FUNDS", "Insufficient funds", 409)
                if action == "deposit" and amount > MAX_MONEY - old:
                    raise BankingError("BALANCE_OVERFLOW", "Balance exceeds 64-bit limit", 409)
                new = old + amount if action == "deposit" else old - amount
                cursor.execute("UPDATE accounts SET balance = %s WHERE account_id = %s", (new, account_id))
        return new

    def transfer_local(self, source: str, destination: str, amount: int, *, owner_epoch: int | None = None) -> tuple[int, int]:
        """Lock account rows in global order to avoid deadlocks across replicas."""
        if source == destination or not self.owns(source) or not self.owns(destination):
            raise ShardIdentityError("Transfer needs two distinct accounts owned by the same shard")
        if type(amount) is not int or not 1 <= amount <= MAX_MONEY:
            raise ValueError("Invalid amount")
        with self._connect() as connection:
            with connection.cursor() as cursor:
                self._verify_identity(cursor)
                self._bind_owner_epoch(cursor, owner_epoch)
                balances = {}
                for account in sorted((source, destination)):
                    cursor.execute("SELECT balance FROM accounts WHERE account_id = %s FOR UPDATE", (account,))
                    row = cursor.fetchone()
                    if row is None:
                        raise BankingError("ACCOUNT_NOT_FOUND", "Account does not exist", 404)
                    balances[account] = int(row[0])
                if balances[source] < amount:
                    raise BankingError("INSUFFICIENT_FUNDS", "Insufficient funds", 409)
                if balances[destination] > MAX_MONEY - amount:
                    raise BankingError("BALANCE_OVERFLOW", "Balance exceeds 64-bit limit", 409)
                debited = balances[source] - amount
                credited = balances[destination] + amount
                cursor.execute("UPDATE accounts SET balance = %s WHERE account_id = %s", (debited, source))
                cursor.execute("UPDATE accounts SET balance = %s WHERE account_id = %s", (credited, destination))
        return debited, credited
