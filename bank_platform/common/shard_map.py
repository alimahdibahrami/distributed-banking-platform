"""Deterministic account ownership used by the Gateway before replica selection."""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path

_ACCOUNT = re.compile(r"^A(\d{4})$")


@dataclass(frozen=True)
class Shard:
    name: str
    first: int
    last: int
    replicas: tuple[str, ...]


@dataclass(frozen=True)
class ShardMap:
    version: int
    shards: tuple[Shard, ...]

    def owner_for(self, account_id: str) -> Shard:
        number = account_number(account_id)
        for shard in self.shards:
            if shard.first <= number <= shard.last:
                return shard
        raise ValueError(f"No shard owner for {account_id}")


def account_number(account_id: str) -> int:
    if not isinstance(account_id, str):
        raise ValueError("Account ID must be a string")
    match = _ACCOUNT.fullmatch(account_id)
    if match is None:
        raise ValueError("Account ID must match A0001 through A2000")
    number = int(match.group(1))
    if not 1 <= number <= 2000:
        raise ValueError("Account ID is outside the assignment range")
    return number


def load_shard_map(path: str | Path) -> ShardMap:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    if type(raw) is not dict or type(raw.get("version")) is not int or raw["version"] < 1:
        raise ValueError("Shard Map must have a positive integer version")
    items = raw.get("shards")
    if not isinstance(items, list) or len(items) != 2:
        raise ValueError("The mandatory baseline must define exactly two shards")
    shards: list[Shard] = []
    for item in items:
        if not isinstance(item, dict):
            raise ValueError("Invalid shard entry")
        first = account_number(item.get("first"))
        last = account_number(item.get("last"))
        replicas = item.get("replicas")
        if first > last or not isinstance(replicas, list) or len(replicas) < 2:
            raise ValueError("Shard range or replica list is invalid")
        if not all(isinstance(r, str) and r.strip() for r in replicas):
            raise ValueError("Replica endpoints must be nonempty strings")
        if len(replicas) != len(set(replicas)):
            raise ValueError("Shard has duplicate replica endpoints")
        name = item.get("id")
        if name not in ("A", "B"):
            raise ValueError("Unexpected shard ID")
        shards.append(Shard(name, first, last, tuple(replicas)))
    if {s.name for s in shards} != {"A", "B"}:
        raise ValueError("Expected unique shards A and B")
    ordered = sorted(shards, key=lambda s: s.first)
    if ordered[0].first != 1 or ordered[0].last != 1000 or ordered[0].name != "A":
        raise ValueError("Shard A must own A0001..A1000")
    if ordered[1].first != 1001 or ordered[1].last != 2000 or ordered[1].name != "B":
        raise ValueError("Shard B must own A1001..A2000")
    endpoints = [replica for s in shards for replica in s.replicas]
    if len(endpoints) != len(set(endpoints)):
        raise ValueError("Replicas may not belong to multiple shards")
    return ShardMap(raw["version"], tuple(ordered))
