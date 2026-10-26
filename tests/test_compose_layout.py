"""Static checks cannot replace an actual `docker compose config`/runtime test."""
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]


def test_compose_service_layout_and_separation():
    config = yaml.safe_load((ROOT / "compose.yaml").read_text(encoding="utf-8"))
    services = config["services"]
    assert config["name"] == "distributed-banking"
    assert set(services) == {"gateway", "coordinator", "bank_a1", "bank_a2", "bank_b1", "bank_b2", "shard_a_db", "shard_b_db", "shard_a_init", "shard_b_init"}
    assert "ports" not in services["shard_a_db"]
    assert "ports" not in services["shard_b_db"]
    assert all("ports" not in services[f"bank_{x}"] for x in ("a1", "a2", "b1", "b2"))
    assert "127.0.0.1" in services["gateway"]["ports"][0]
    assert set(config["volumes"]) == {"shard_a_data", "shard_b_data", "coordinator_state"}
    for shard in ("a", "b"):
        for replica in ("1", "2"):
            service = services[f"bank_{shard}{replica}"]
            assert service["environment"]["BANK_DB_HOST"] == f"shard_{shard}_db"
            assert service["restart"] == "no"


def test_postgres_config_has_prepared_transactions():
    services = yaml.safe_load((ROOT / "compose.yaml").read_text(encoding="utf-8"))["services"]
    for db in ("shard_a_db", "shard_b_db"):
        assert "max_prepared_transactions=100" in services[db]["command"]
