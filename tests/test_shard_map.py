import json
from pathlib import Path

import pytest

from bank_platform.common.shard_map import account_number, load_shard_map


CONFIG = Path(__file__).resolve().parents[1] / "config" / "shard_map.json"


@pytest.fixture
def mapping():
    return load_shard_map(CONFIG)


@pytest.mark.parametrize("account,expected", [
    ("A0001", "A"), ("A0500", "A"), ("A1000", "A"),
    ("A1001", "B"), ("A1500", "B"), ("A2000", "B"),
])
def test_owner_boundaries(mapping, account, expected):
    assert mapping.owner_for(account).name == expected


@pytest.mark.parametrize("value", ["A0000", "A2001", "B1000", "A100", "A10000", "a0100", " A0001", None, 25])
def test_invalid_account_rejected(value):
    with pytest.raises(ValueError):
        account_number(value)


def test_all_accounts_have_exactly_one_owner(mapping):
    assert mapping.version == 1
    for number in range(1, 2001):
        account = f"A{number:04d}"
        owners = [s for s in mapping.shards if s.first <= number <= s.last]
        assert len(owners) == 1
        assert mapping.owner_for(account) == owners[0]
    assert all(len(s.replicas) == 2 for s in mapping.shards)


def test_bad_map_overlapping_range_rejected(tmp_path):
    raw = json.loads(CONFIG.read_text(encoding="utf-8"))
    raw["shards"][1]["first"] = "A1000"
    path = tmp_path / "bad.json"
    path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(ValueError, match="Shard B"):
        load_shard_map(path)


def test_duplicate_replica_rejected(tmp_path):
    raw = json.loads(CONFIG.read_text(encoding="utf-8"))
    raw["shards"][1]["replicas"][0] = raw["shards"][0]["replicas"][0]
    path = tmp_path / "bad.json"
    path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(ValueError, match="multiple shards"):
        load_shard_map(path)
