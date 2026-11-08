"""Validated account ownership map.

Version 1 remains the immutable two-shard assignment baseline. Version 2+
may express the *post-cutover* A/B/C ranges, but publishing a JSON file is
NOT an online cutover: a durable shard-level epoch fence is required first.
"""
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
    if not isinstance(items, list) or len(items) not in (2, 3):
        raise ValueError("Shard Map must contain two or three shards")
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
        if name not in ("A", "B", "C"):
            raise ValueError("Unexpected shard ID")
        shards.append(Shard(name, first, last, tuple(replicas)))
    ordered = tuple(sorted(shards, key=lambda s: s.first))
    # No holes, overlaps, duplicate IDs or duplicate replicas are legal.
    if len({s.name for s in ordered}) != len(ordered):
        raise ValueError("Duplicate shard ID")
    if ordered[0].first != 1 or ordered[-1].last != 2000:
        raise ValueError("Map must cover A0001..A2000")
    if any(left.last + 1 != right.first for left, right in zip(ordered, ordered[1:])):
        raise ValueError("Shard B or C ranges must have no gaps or overlaps")
    endpoints = [replica for s in ordered for replica in s.replicas]
    if len(endpoints) != len(set(endpoints)):
        raise ValueError("Replicas may not belong to multiple shards")
    if len(ordered) == 2:
        expected = (("A", 1, 1000), ("B", 1001, 2000))
        if tuple((s.name, s.first, s.last) for s in ordered) != expected:
            raise ValueError("Baseline must retain A0001..A1000 / A1001..A2000")
    else:
        expected = (("A", 1, 1000), ("B", 1001, 1500), ("C", 1501, 2000))
        if raw["version"] < 2:
            raise ValueError("Three-shard map requires epoch/version >=2")
        if tuple((s.name, s.first, s.last) for s in ordered) != expected:
            raise ValueError("Three-shard map must move A1501..A2000 from B to C")
    return ShardMap(raw["version"], ordered)
