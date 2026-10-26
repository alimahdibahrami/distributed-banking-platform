"""Fast API tests, with an injected store; these do not require PostgreSQL."""
from __future__ import annotations

import asyncio
import sys
from types import SimpleNamespace

import httpx
import pytest

from bank_platform.shard.app import create_app
from bank_platform.shard.storage import DbConfig, PostgresAccountStore, ShardIdentityError


def get(app, path):
    async def request():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                     base_url="http://test") as client:
            return await client.get(path)
    return asyncio.run(request())


class FakeStore:
    def __init__(self, shard):
        self.shard = shard
        self.values = {"A0500": 1000, "A1500": 500}
        self.failure = False

    def owns(self, account_id):
        n = int(account_id[1:])
        return (1 <= n <= 1000) if self.shard == "A" else (1001 <= n <= 2000)

    def ready(self):
        if self.failure:
            raise OSError("DB down")
        return True

    def read_account(self, account_id):
        if self.failure:
            raise OSError("DB down")
        return self.values.get(account_id)


@pytest.mark.parametrize("shard,replica,account,balance", [
    ("A", "A1", "A0500", 1000), ("A", "A2", "A0500", 1000),
    ("B", "B1", "A1500", 500), ("B", "B2", "A1500", 500),
])
def test_phase2_storage_is_read_only_and_shard_owned(shard, replica, account, balance):
    store = FakeStore(shard)
    app = create_app(shard, replica, store)
    assert get(app, "/ready").json() == {
        "status": "ready", "shard": shard, "replica": replica,
        "storage_ready": True, "banking_ready": False,
    }
    result = get(app, f"/internal/accounts/{account}")
    assert result.status_code == 200
    assert result.json() == {"account": account, "balance": balance,
                             "shard": shard, "replica": replica}
    assert get(app, "/internal/accounts/Aabc").status_code == 400
    assert get(app, "/internal/accounts/../../etc").status_code != 200
    other = "A1500" if shard == "A" else "A0500"
    wrong = get(app, f"/internal/accounts/{other}")
    assert wrong.status_code == 409
    assert wrong.json()["error"] == "WRONG_SHARD"


def test_two_replicas_share_a_single_authoritative_storage():
    shared_store = FakeStore("A")
    a1 = create_app("A", "A1", shared_store)
    a2 = create_app("A", "A2", shared_store)
    shared_store.values["A0500"] = 812
    assert get(a1, "/internal/accounts/A0500").json()["balance"] == 812
    assert get(a2, "/internal/accounts/A0500").json()["balance"] == 812


def test_replica_cannot_fake_readiness_when_db_down():
    store = FakeStore("A")
    store.failure = True
    app = create_app("A", "A1", store)
    assert get(app, "/health").status_code == 200
    ready = get(app, "/ready")
    assert ready.status_code == 503
    assert ready.json()["storage_ready"] is False
    assert get(app, "/internal/accounts/A0500").status_code == 503


def test_account_not_found_inside_correct_shard():
    store = FakeStore("A")
    assert get(create_app("A", "A1", store), "/internal/accounts/A0501").status_code == 404


@pytest.mark.parametrize("account,expected", [
    ("A0001", True), ("A1000", True), ("A1001", False), ("A2000", False),
])
def test_store_checks_owner_before_database_query(account, expected):
    store = PostgresAccountStore("A", DbConfig("unused", "unused", "unused", "unused"))
    assert store.owns(account) is expected
    if not expected:
        with pytest.raises(ShardIdentityError):
            store.read_account(account)


def test_database_query_is_parameterized_and_identity_checked(monkeypatch):
    queries = []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def execute(self, query, params=None):
            queries.append((query, params))

        def fetchone(self):
            if "FROM shard_metadata" in queries[-1][0]:
                return ("A", 1, 1000, 1)
            return (987,)

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def cursor(self):
            return Cursor()

    connection_kwargs = []

    def connect(**kwargs):
        connection_kwargs.append(kwargs)
        return Connection()

    monkeypatch.setitem(sys.modules, "psycopg", SimpleNamespace(connect=connect))
    store = PostgresAccountStore("A", DbConfig("db-host", "bank_a", "bank", "test-pass"))
    assert store.read_account("A0500") == 987
    assert queries[0][0].startswith("SELECT shard_id")
    assert queries[1][1] == ("A0500",)
    assert "account_id = %s" in queries[1][0]
    assert connection_kwargs[0]["host"] == "db-host"


def test_wrong_database_identity_rejected(monkeypatch):
    class WrongCursor:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def execute(self, *args):
            pass

        def fetchone(self):
            return ("B", 1001, 2000, 1)

    class WrongConnection:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def cursor(self):
            return WrongCursor()

    monkeypatch.setitem(sys.modules, "psycopg", SimpleNamespace(connect=lambda **kwargs: WrongConnection()))
    store = PostgresAccountStore("A", DbConfig("host", "db", "user", "pass"))
    with pytest.raises(ShardIdentityError):
        store.read_account("A0500")
