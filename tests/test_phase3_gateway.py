"""Gateway selection, strict shard isolation, readiness and read-only failover."""
import asyncio
import logging
from collections import Counter

import httpx

from bank_platform.gateway.app import create_app


class FakeReplicas:
    def __init__(self):
        self.ready = {name: True for name in ("A1", "A2", "B1", "B2")}
        self.fail_reads = set()
        self.requests = []
        self.wrong_identity = set()

    def __call__(self, request: httpx.Request) -> httpx.Response:
        name = request.url.host.replace("bank_", "").upper()
        shard = name[0]
        self.requests.append((name, request.url.path))
        if request.url.path == "/ready":
            if not self.ready[name]:
                return httpx.Response(503, json={"status": "unavailable"})
            reported = "B" if name in self.wrong_identity and shard == "A" else shard
            return httpx.Response(200, json={"status": "ready", "storage_ready": True,
                                             "shard": reported, "replica": name})
        if request.url.path.startswith("/internal/operations") or request.url.path.startswith("/internal/transfers"):
            return httpx.Response(503, json={"error": "NOT_IMPLEMENTED"})
        if name in self.fail_reads or not self.ready[name]:
            return httpx.Response(503, json={"error": "STORAGE_UNAVAILABLE"})
        account = request.url.path.rsplit("/", 1)[-1]
        if (account[1:] <= "1000") != (shard == "A"):
            raise AssertionError("Wrong-shard route attempted")
        return httpx.Response(200, json={"account": account, "shard": shard,
                                         "replica": name, "balance": 1000 if shard == "A" else 500})


async def exercise(callback):
    upstream = FakeReplicas()
    app = create_app(transport=httpx.MockTransport(upstream), poll_interval=3600)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                     base_url="http://gateway") as client:
            return await callback(client, app.state.router, upstream)


def run(callback):
    return asyncio.run(exercise(callback))


def test_round_robin_is_independent_per_shard_and_logged(caplog):
    async def check(client, router, upstream):
        caplog.set_level(logging.INFO, logger="uvicorn.error")
        values = []
        for account in ("A0500", "A1500", "A0500", "A1500", "A0500", "A1500"):
            r = await client.get(f"/api/v1/accounts/{account}/balance")
            assert r.status_code == 200
            values.append(r.json()["replica"])
        assert values == ["A1", "B1", "A2", "B2", "A1", "B1"]
        assert router.available("A") == router.available("B") == 2
        assert "gateway.route account=A0500 shard=A replica=A1" in caplog.text
        r = await client.get("/ready")
        assert r.status_code == 200
        assert r.json() == {"status": "ready", "service": "gateway",
                            "routing_ready": True, "banking_ready": False,
                            "healthy_replicas": {"A": 2, "B": 2}}
        r = await client.get("/health")
        assert r.json()["banking_ready"] is False
    run(check)


def test_post_balance_and_writes_never_fall_back_to_read_path():
    async def check(client, router, upstream):
        response = await client.post("/api/v1/operations", json={"action": "balance", "account": "A0500"})
        assert response.status_code == 200
        assert response.json()["balance"] == 1000
        for action in ("deposit", "withdraw"):
            result = await client.post("/api/v1/operations", json={"action": action,
                                                                   "account": "A0500", "amount": 10})
            # Phase 3 fake upstream does not implement the new operation contract.
            # An unsuccessful write must not be silently treated as success.
            assert result.status_code != 200
        assert any("/internal/operations" in path for _, path in upstream.requests)
    run(check)


def test_invalid_account_never_hits_a_replica():
    async def check(client, router, upstream):
        before = len(upstream.requests)
        for account in ("A0000", "A2001", "B0001", "A05000", "bad", "A-500"):
            response = await client.get(f"/api/v1/accounts/{account}/balance")
            assert response.status_code == 400
            assert response.json()["error"]["code"] == "INVALID_ACCOUNT"
        for payload in ({"action": "balance", "account": "A0500", "amount": 1},
                        {"action": "deposit", "account": "A0500", "amount": -5},
                        {"action": "balance", "account": "A2001"}):
            response = await client.post("/api/v1/operations", json=payload)
            assert response.status_code == 400
        assert len(upstream.requests) == before
    run(check)


def test_unhealthy_replica_is_excluded_and_restored_after_reprobe():
    async def check(client, router, upstream):
        upstream.ready["A1"] = False
        await router.refresh()
        assert router.available("A") == 1
        reads = [await client.get("/api/v1/accounts/A0500/balance") for _ in range(8)]
        assert {r.json()["replica"] for r in reads} == {"A2"}
        upstream.ready["A2"] = False
        await router.refresh()
        response = await client.get("/api/v1/accounts/A0500/balance")
        assert response.status_code == 503
        assert response.json()["error"]["code"] == "NO_HEALTHY_REPLICA"
        response = await client.get("/api/v1/accounts/A1500/balance")
        assert response.status_code == 200
        assert response.json()["shard"] == "B"
        assert (await client.get("/ready")).status_code == 503
        upstream.ready["A1"] = True
        await router.refresh()
        response = await client.get("/api/v1/accounts/A0500/balance")
        assert response.json()["replica"] == "A1"
        assert (await client.get("/ready")).status_code == 200
    run(check)


def test_active_replica_fails_and_same_request_tries_peer_only():
    async def check(client, router, upstream):
        upstream.fail_reads.add("A1")
        response = await client.get("/api/v1/accounts/A0500/balance")
        assert response.status_code == 200
        assert response.json()["replica"] == "A2"
        assert router.healthy["A1"] is False
        assert upstream.requests[-2:] == [
            ("A1", "/internal/accounts/A0500"), ("A2", "/internal/accounts/A0500")]
        upstream.fail_reads.add("A2")
        assert (await client.get("/api/v1/accounts/A0500/balance")).status_code == 503
        assert (await client.get("/api/v1/accounts/A1500/balance")).status_code == 200
    run(check)


def test_cross_shard_probe_claim_is_rejected():
    async def check(client, router, upstream):
        upstream.wrong_identity.add("A1")
        await router.refresh()
        assert router.available("A") == 1
        for _ in range(5):
            response = await client.get("/api/v1/accounts/A0500/balance")
            assert response.json()["replica"] == "A2"
    run(check)


def test_many_concurrent_reads_use_both_replicas_of_each_shard():
    async def check(client, router, upstream):
        accounts = ["A0500", "A1500"] * 40
        responses = await asyncio.gather(*[
            client.get(f"/api/v1/accounts/{account}/balance") for account in accounts
        ])
        assert all(r.status_code == 200 for r in responses)
        counts = Counter(r.json()["replica"] for r in responses)
        assert counts == {"A1": 20, "A2": 20, "B1": 20, "B2": 20}
        assert all(r.json()["account"] == account for r, account in zip(responses, accounts))
    run(check)


def test_network_connect_error_is_retried_only_inside_owning_shard():
    upstream = FakeReplicas()

    def transport_handler(request):
        if request.url.host == "bank_a1" and request.url.path.startswith("/internal/accounts/"):
            raise httpx.ConnectError("Connection refused", request=request)
        return upstream(request)

    async def check():
        app = create_app(transport=httpx.MockTransport(transport_handler), poll_interval=3600)
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                         base_url="http://gateway") as client:
                result = await client.get("/api/v1/accounts/A0500/balance")
                assert result.status_code == 200
                assert result.json()["replica"] == "A2"
                assert app.state.router.healthy["A1"] is False
                assert not any(name.startswith("B") and path.endswith("/A0500")
                               for name, path in upstream.requests)
    asyncio.run(check())
