"""Phase 8: readiness reflects live coordinator + available shard replicas."""
import asyncio

import httpx

from bank_platform.gateway.app import create_app
from tests.test_phase3_gateway import FakeReplicas


def run_case(callback):
    async def scenario():
        upstream = FakeReplicas()
        app = create_app(transport=httpx.MockTransport(upstream), poll_interval=3600)
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                         base_url="http://gateway") as client:
                await callback(client, app, upstream)
    asyncio.run(scenario())


def test_gateway_readiness_requires_coordinator():
    async def check(client, app, upstream):
        ready = await client.get("/ready")
        assert ready.status_code == 200
        assert ready.json()["banking_ready"] is True
        upstream.coordinator_ready = False
        response = await client.get("/ready")
        assert response.status_code == 503
        assert response.json()["routing_ready"] is True
        assert response.json()["coordinator_ready"] is False
        assert response.json()["banking_ready"] is False
        assert (await client.get("/health")).status_code == 200
    run_case(check)


def test_gateway_readiness_requires_both_shards_and_coordinator():
    async def check(client, app, upstream):
        upstream.ready["A1"] = False
        upstream.ready["A2"] = False
        await app.state.router.refresh()
        response = await client.get("/ready")
        assert response.status_code == 503
        assert response.json()["coordinator_ready"] is True
        assert response.json()["routing_ready"] is False
        assert response.json()["banking_ready"] is False
        assert response.json()["healthy_replicas"] == {"A": 0, "B": 2}
        upstream.ready["A2"] = True
        await app.state.router.refresh()
        response = await client.get("/ready")
        assert response.status_code == 200
        assert response.json()["banking_ready"] is True
    run_case(check)


def test_coordinator_probe_is_fail_closed():
    async def scenario():
        upstream = FakeReplicas()
        def handler(req):
            if req.url.host == "coordinator":
                raise httpx.ConnectError("coordinator offline", request=req)
            return upstream(req)
        app = create_app(transport=httpx.MockTransport(handler), poll_interval=3600)
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                         base_url="http://gateway") as client:
                r = await client.get("/ready")
                assert r.status_code == 503
                assert r.json()["routing_ready"] is True
                assert r.json()["coordinator_ready"] is False
                assert r.json()["banking_ready"] is False
    asyncio.run(scenario())
