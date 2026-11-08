"""Fail-closed, opt-in routing-epoch agreement at the API boundary.

This gate prevents old/new Gateway and Coordinator instances from accepting
new financial requests under divergent shard maps. It is NOT a database
fencing proof or permission to publish a new shard map.
"""
from __future__ import annotations

import os

import httpx

from bank_platform.migration.epoch import enabled as epoch_propagation_enabled


def strict_routing() -> bool:
    return os.getenv('BANK_REQUIRE_ROUTING_EPOCH', '0') == '1'


def assert_configuration() -> None:
    if strict_routing() and not epoch_propagation_enabled():
        raise ValueError('BANK_REQUIRE_ROUTING_EPOCH requires BANK_EPOCH_PROPAGATION=1')


def epoch_agrees(payload: object, required: int) -> bool:
    """Strict identity, readiness, and type checks; bool is not a valid epoch."""
    return (isinstance(payload, dict)
            and payload.get('status') == 'alive'
            and payload.get('service') == 'coordinator'
            and payload.get('transactions_ready') is True
            and type(payload.get('routing_epoch')) is int
            and payload['routing_epoch'] == required)


async def probe_coordinator_epoch(client: httpx.AsyncClient | None, url: str,
                                  required: int) -> bool:
    if client is None:
        return False
    try:
        response = await client.get(url.rstrip('/') + '/health', timeout=2.0)
        return response.status_code == 200 and epoch_agrees(response.json(), required)
    except (httpx.HTTPError, ValueError, TypeError):
        return False
