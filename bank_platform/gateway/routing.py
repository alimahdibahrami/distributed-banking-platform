"""Health-aware, per-shard Round Robin routing for read-only account inquiries.

The gateway monitors replica /ready (not /health). It never selects a
replica outside the account's owning shard. In this phase, failover retries
are allowed ONLY for reads; mutating operations require later idempotency work.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

import httpx

from bank_platform.common.shard_map import Shard, ShardMap

LOGGER = logging.getLogger("uvicorn.error")  # Visible in Docker/Uvicorn logs


@dataclass(frozen=True)
class Replica:
    shard: str
    name: str
    url: str


class ReplicaRouter:
    def __init__(self, mapping: ShardMap, client: httpx.AsyncClient):
        self.client = client
        self.replicas: dict[str, tuple[Replica, ...]] = {
            shard.name: tuple(
                Replica(shard.name, f"{shard.name}{index}", endpoint.rstrip("/"))
                for index, endpoint in enumerate(shard.replicas, start=1)
            )
            for shard in mapping.shards
        }
        self.healthy = {replica.name: False for group in self.replicas.values() for replica in group}
        self._position = {shard: 0 for shard in self.replicas}

    async def refresh(self) -> None:
        """Check every backend independently, including previously failed backends."""
        probes = [(replica, asyncio.create_task(self._probe(replica)))
                  for group in self.replicas.values() for replica in group]
        for replica, task in probes:
            healthy = await task
            self.set_health(replica, healthy)

    async def _probe(self, replica: Replica) -> bool:
        try:
            response = await self.client.get(f"{replica.url}/ready")
            if response.status_code != 200:
                return False
            payload = response.json()
            return (isinstance(payload, dict) and payload.get("status") == "ready"
                    and payload.get("storage_ready") is True
                    and payload.get("shard") == replica.shard
                    and payload.get("replica") == replica.name)
        except (httpx.HTTPError, ValueError, TypeError):
            return False

    def set_health(self, replica: Replica, healthy: bool) -> None:
        previous = self.healthy[replica.name]
        self.healthy[replica.name] = healthy
        if previous != healthy:
            LOGGER.warning("gateway.replica_health shard=%s replica=%s healthy=%s",
                           replica.shard, replica.name, healthy)

    def available(self, shard: str) -> int:
        return sum(self.healthy[r.name] for r in self.replicas[shard])

    def all_shards_available(self) -> bool:
        return all(self.available(shard) > 0 for shard in self.replicas)

    def next_replica(self, shard: Shard, excluded: set[str]) -> Replica | None:
        """Round Robin cursor is independent per shard; no awaits before update."""
        group = self.replicas[shard.name]
        start = self._position[shard.name]
        for offset in range(len(group)):
            index = (start + offset) % len(group)
            replica = group[index]
            if replica.name not in excluded and self.healthy[replica.name]:
                self._position[shard.name] = (index + 1) % len(group)
                return replica
        return None

    async def read_balance(self, account: str, shard: Shard) -> dict | None:
        """Return a verified read result or None when same-shard backends fail."""
        if self.available(shard.name) == 0:
            # A newly started replica need not wait for the next poll tick.
            await self.refresh()
        attempted: set[str] = set()
        while (replica := self.next_replica(shard, attempted)) is not None:
            attempted.add(replica.name)
            try:
                response = await self.client.get(
                    f"{replica.url}/internal/accounts/{account}",
                )
                if response.status_code == 404:
                    # An absent account is not evidence of a dead replica.
                    return {"not_found": True}
                if response.status_code != 200:
                    raise ValueError(f"Unexpected replica HTTP status {response.status_code}")
                payload = response.json()
                if not isinstance(payload, dict) or (
                    payload.get("account") != account
                    or payload.get("shard") != shard.name
                    or payload.get("replica") != replica.name
                    or type(payload.get("balance")) is not int
                    or payload["balance"] < 0
                ):
                    raise ValueError("Replica returned a mismatched account, identity or balance")
                LOGGER.info("gateway.route account=%s shard=%s replica=%s",
                            account, shard.name, replica.name)
                return payload
            except (httpx.HTTPError, ValueError, TypeError) as exc:
                LOGGER.warning("gateway.route_failed account=%s shard=%s replica=%s reason=%s",
                               account, shard.name, replica.name, type(exc).__name__)
                self.set_health(replica, False)
                # Retry is safe here because this request is read-only.
        return None


async def poll_health(router: ReplicaRouter, interval_seconds: float) -> None:
    """Keep the pool current; cancellation is handled by the app lifespan."""
    while True:
        try:
            await router.refresh()
        except Exception:
            LOGGER.exception("Unexpected gateway health polling failure")
        await asyncio.sleep(interval_seconds)
