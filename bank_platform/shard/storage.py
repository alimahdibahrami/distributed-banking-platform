"""PostgreSQL-backed read-only storage for phase 2.

No banking writes are exposed before phase 4. Each replica connects to its
shard's single authoritative PostgreSQL instance; balances are never cached.
"""
from __future__ import annotations

import os
from dataclasses import dataclass

from bank_platform.common.shard_map import account_number


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
        if shard_id not in {"A", "B"}:
            raise ValueError("Unknown shard")
        self.shard_id = shard_id
        self.config = config or DbConfig.from_env()
        self.first, self.last = (1, 1000) if shard_id == "A" else (1001, 2000)

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
