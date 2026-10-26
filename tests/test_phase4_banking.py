"""Phase 4: local write validation, SQL locking and non-retrying Gateway."""
from __future__ import annotations

import asyncio
import json
from collections import Counter

import httpx
import pytest

from bank_platform.common.validation import BankingError, MAX_MONEY, validate_transfer
from bank_platform.gateway.app import create_app as gateway_app
from bank_platform.shard.app import create_app as bank_app
from bank_platform.shard.storage import DbConfig, PostgresAccountStore, ShardIdentityError


async def request(app, method, path, **kwargs):
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://app") as client:
        return await client.request(method, path, **kwargs)


def run(coro):
    return asyncio.run(coro)


def test_transfer_validation_and_account_boundaries():
    valid = {"action": "transfer", "from_account": "A0001", "to_account": "A2000", "amount": 3}
    assert validate_transfer(valid).amount == 3
    for change in ({"to_account": "A0001"}, {"from_account": "A0000"},
                   {"amount": True}, {"amount": 0}, {"amount": MAX_MONEY+1},
                   {"to_account": "B1900"}, {"bad": 1}):
        invalid = {**valid, **change}
        with pytest.raises(BankingError):
            validate_transfer(invalid)


class FakeStore:
    def __init__(self, shard="A"):
        self.shard = shard
        self.data = {"A0001": 100, "A0002": 250} if shard == "A" else {"A1001": 100, "A1002": 250}

    def owns(self, account):
        return account in self.data

    def ready(self):
        return True

    def read_account(self, account):
        return self.data.get(account)

    def mutate(self, action, account, amount):
        old = self.data[account]
        if action == "withdraw" and amount > old:
            raise BankingError("INSUFFICIENT_FUNDS", "No funds", 409)
        self.data[account] += amount if action == "deposit" else -amount
        return self.data[account]

    def transfer_local(self, source, destination, amount):
        if amount > self.data[source]:
            raise BankingError("INSUFFICIENT_FUNDS", "No funds", 409)
        self.data[source] -= amount
        self.data[destination] += amount
        return self.data[source], self.data[destination]


def test_replica_local_mutations_and_transfer_validation():
    store = FakeStore()
    app = bank_app("A", "A1", store)
    async def check():
        r = await request(app, "POST", "/internal/operations", json={"action":"deposit","account":"A0001","amount":9})
        assert r.status_code == 200 and r.json()["balance"] == 109
        r = await request(app, "POST", "/internal/operations", json={"action":"withdraw","account":"A0001","amount":3})
        assert r.status_code == 200 and r.json()["balance"] == 106
        r = await request(app, "POST", "/internal/operations", json={"action":"withdraw","account":"A0001","amount":1000})
        assert r.status_code == 409 and r.json()["error"] == "INSUFFICIENT_FUNDS"
        r = await request(app, "POST", "/internal/operations", json={"action":"deposit","account":"A1001","amount":1})
        assert r.status_code == 409 and r.json()["error"] == "WRONG_SHARD"
        r = await request(app, "POST", "/internal/transfers", json={"action":"transfer","from_account":"A0001","to_account":"A0002","amount":20})
        assert r.status_code == 200 and r.json()["from_balance"] == 86 and r.json()["to_balance"] == 270
        r = await request(app, "POST", "/internal/transfers", json={"action":"transfer","from_account":"A0001","to_account":"A1001","amount":2})
        assert r.status_code == 409 and r.json()["error"] == "WRONG_SHARD"
    run(check())


class FakePgCursor:
    def __init__(self, connection):
        self.connection = connection
        self.current = None
    def __enter__(self): return self
    def __exit__(self, *args): pass
    def execute(self, query, params=None):
        self.connection.queries.append((query, params))
        if "FROM shard_metadata" in query:
            self.current = ("A", 1, 1000, 1)
        elif "SELECT balance" in query:
            self.current = (self.connection.pending[params[0]],) if params[0] in self.connection.pending else None
        elif "UPDATE accounts" in query:
            self.connection.update_count += 1
            self.connection.pending[params[1]] = params[0]
            if self.connection.fail_second and self.connection.update_count == 2:
                raise RuntimeError("injected SQL failure after first update")
    def fetchone(self): return self.current

class FakePgConnection:
    def __init__(self, db, fail_second=False):
        self.db = db
        self.pending = dict(db)
        self.queries = []
        self.fail_second = fail_second
        self.update_count = 0
    def __enter__(self): return self
    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            self.db.update(self.pending)
    def cursor(self): return FakePgCursor(self)


def test_sql_transactions_lock_rows_and_commit_atomically():
    store = PostgresAccountStore("A", DbConfig("host","dbname","user","password"))
    db = {"A0001": 100, "A0002": 250}
    connections = []
    def connect():
        conn = FakePgConnection(db)
        connections.append(conn)
        return conn
    store._connect = connect
    assert store.mutate("deposit", "A0001", 10) == 110
    assert store.mutate("withdraw", "A0001", 20) == 90
    assert store.transfer_local("A0002", "A0001", 50) == (200, 140)
    assert db == {"A0001": 140, "A0002": 200}
    locked = [params[0] for query, params in connections[-1].queries if "FOR UPDATE" in query]
    assert locked == ["A0001", "A0002"]  # global order, independent of transfer direction
    with pytest.raises(BankingError) as ex:
        store.transfer_local("A0001", "A0002", 1000)
    assert ex.value.code == "INSUFFICIENT_FUNDS"
    assert db == {"A0001": 140, "A0002": 200}


def test_second_update_failure_rolls_back_both_local_transfer_legs():
    store = PostgresAccountStore("A", DbConfig("host","dbname","user","password"))
    db = {"A0001": 100, "A0002": 250}
    store._connect = lambda: FakePgConnection(db, fail_second=True)
    with pytest.raises(RuntimeError, match="injected SQL failure"):
        store.transfer_local("A0001", "A0002", 10)
    assert db == {"A0001": 100, "A0002": 250}


def test_storage_wrong_owner_and_overflow_no_write():
    store = PostgresAccountStore("A", DbConfig("host","dbname","user","password"))
    db = {"A0001": MAX_MONEY, "A0002": 50}
    store._connect = lambda: FakePgConnection(db)
    with pytest.raises(BankingError) as e:
        store.mutate("deposit", "A0001", 1)
    assert e.value.code == "BALANCE_OVERFLOW"
    with pytest.raises(BankingError):
        store.transfer_local("A0002", "A0001", 1)
    with pytest.raises(ShardIdentityError):
        store.mutate("deposit", "A1001", 1)
    assert db["A0001"] == MAX_MONEY and db["A0002"] == 50


class GatewayFake:
    def __init__(self):
        self.writes = []
        self.fail_write = False
    def __call__(self, req):
        name = req.url.host.replace("bank_", "").upper()
        if req.url.path == "/ready":
            return httpx.Response(200, json={"status":"ready", "shard":name[0],"replica":name,"storage_ready":True})
        if req.url.path == "/internal/operations" or req.url.path == "/internal/transfers":
            self.writes.append((name, req.url.path, json.loads(req.content)))
            if self.fail_write:
                raise httpx.ReadTimeout("response lost after possible commit", request=req)
            return httpx.Response(200, json={"shard":name[0], "replica":name, "balance":1010})
        return httpx.Response(200, json={"account":"A0001","shard":name[0],"replica":name,"balance":1000})


def test_gateway_local_transfer_and_cross_shard_coordinator_unavailable():
    upstream = GatewayFake()
    app = gateway_app(transport=httpx.MockTransport(upstream), poll_interval=3600)
    async def check():
        async with app.router.lifespan_context(app):
            body = {"action":"transfer","from_account":"A0001","to_account":"A0002","amount":5}
            same = await request(app, "POST", "/api/v1/operations", json=body)
            assert same.status_code == 200 and same.json()["shard"] == "A"
            other = await request(app, "POST", "/api/v1/operations", json={**body, "to_account":"A1500"})
            assert other.status_code == 503 and other.json()["error"]["code"] == "COORDINATOR_UNAVAILABLE"
            assert len(upstream.writes) == 1 and upstream.writes[0][0].startswith("A")
            d = await request(app, "POST", "/api/v1/operations", json={"action":"deposit","account":"A1500","amount":5})
            assert d.status_code == 200 and d.json()["shard"] == "B"
            assert len(upstream.writes) == 2 and upstream.writes[1][0].startswith("B")
    run(check())


def test_write_timeout_unknown_outcome_never_retried_on_peer():
    upstream = GatewayFake()
    upstream.fail_write = True
    app = gateway_app(transport=httpx.MockTransport(upstream), poll_interval=3600)
    async def check():
        async with app.router.lifespan_context(app):
            result = await request(app,"POST","/api/v1/operations", json={"action":"withdraw","account":"A0001","amount":1})
            assert result.status_code == 504
            assert result.json()["error"]["code"] == "UNKNOWN_OUTCOME"
            assert len(upstream.writes) == 1
            assert upstream.writes[0][0] in ("A1", "A2")
    run(check())
