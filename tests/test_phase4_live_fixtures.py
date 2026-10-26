"""Regression: live smoke tests use the canonical A-prefixed account IDs."""
import re
from pathlib import Path

from bank_platform.common.shard_map import account_number


def test_phase4_live_fixture_account_ids_are_valid():
    script = Path(__file__).resolve().parents[1] / "scripts" / "verify_phase4.py"
    account_ids = set(re.findall(r'"([AB][0-9]{4})"', script.read_text(encoding="utf-8")))
    assert account_ids == {"A0701", "A0702", "A1701", "A1702"}
    for account_id in account_ids:
        assert 1 <= account_number(account_id) <= 2000
