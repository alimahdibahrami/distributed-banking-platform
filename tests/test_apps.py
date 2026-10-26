import asyncio

import httpx
import pytest

from bank_platform.gateway.app import create_app as gateway_app
from bank_platform.shard.app import create_app as bank_app
from bank_platform.coordinator.app import create_app as coordinator_app


def invoke(app, method, path, **kwargs):
    async def _send():
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            return await client.request(method, path, **kwargs)
    return asyncio.run(_send())


def test_gateway_liveness_and_no_fake_success():
    app = gateway_app()
    assert invoke(app, "GET", "/health").json() == {
        "status": "alive", "service": "gateway", "banking_ready": False
    }
    response = invoke(app, "POST", "/api/v1/operations", json={"action": "withdraw", "account": "A0001", "amount": 1})
    assert response.status_code == 501
    assert response.json()["error"]["code"] == "NOT_IMPLEMENTED"


@pytest.mark.parametrize("shard,replica", [("A", "A1"), ("A", "A2"), ("B", "B1"), ("B", "B2")])
def test_replica_identity_and_no_fake_success(shard, replica):
    app = bank_app(shard, replica)
    result = invoke(app, "GET", "/health")
    assert result.status_code == 200
    assert result.json() == {"status": "alive", "service": "bank", "shard": shard,
                             "replica": replica, "banking_ready": False}
    result = invoke(app, "POST", "/internal/operations")
    assert result.status_code == 501


def test_invalid_replica_identity_rejected():
    with pytest.raises(ValueError):
        bank_app("A", "B1")


def test_coordinator_liveness_and_no_fake_success():
    app = coordinator_app()
    assert invoke(app, "GET", "/health").json()["transactions_ready"] is False
    assert invoke(app, "POST", "/internal/transactions").status_code == 501
