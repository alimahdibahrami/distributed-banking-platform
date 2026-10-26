"""Check declarative safety; real Postgres validation runs on the VPS."""
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def test_phase2_schema_identity_range_and_seed_protection():
    for shard, first, last in (("a", 1, 1000), ("b", 1001, 2000)):
        sql = (ROOT / "database" / f"shard_{shard}.sql").read_text()
        assert "BEGIN;" in sql and "COMMIT;" in sql
        assert f"BETWEEN {first} AND {last}" in sql
        assert "balance BIGINT NOT NULL CHECK (balance >= 0)" in sql
        assert "ON CONFLICT (account_id) DO NOTHING" in sql
        assert "CREATE TABLE IF NOT EXISTS accounts" in sql
        assert "shard_metadata" in sql and "schema_version" in sql
        assert "RAISE EXCEPTION" in sql
        assert "bootstrap_history" in sql and "first_seed" in sql
    assert "WHEN n = 1500 THEN 500" in (ROOT / "database/shard_b.sql").read_text()


def test_phase2_compose_db_bootstrap_and_healthy_replica_dependencies():
    cfg = yaml.safe_load((ROOT / "compose.yaml").read_text())
    services = cfg["services"]
    assert len(services) == 10  # 8 long-lived services + 2 one-shot initializers
    for sid in ("a", "b"):
        init = services[f"shard_{sid}_init"]
        assert init["depends_on"][f"shard_{sid}_db"]["condition"] == "service_healthy"
        assert init["environment"]["PGHOST"] == f"shard_{sid}_db"
        assert init["environment"]["PGDATABASE"] == f"bank_{sid}"
        assert "ON_ERROR_STOP=1" in init["command"]
        assert any(f"database/shard_{sid}.sql" in str(v) and str(v).endswith(":ro")
                   for v in init["volumes"])
        assert "ports" not in init
        for suffix in ("1", "2"):
            service = services[f"bank_{sid}{suffix}"]
            assert service["depends_on"][f"shard_{sid}_init"]["condition"] == "service_completed_successfully"
            assert "/ready" in " ".join(service["healthcheck"]["test"])
            assert service["environment"]["BANK_DB_HOST"] == f"shard_{sid}_db"
            assert service["environment"]["BANK_DB_NAME"] == f"bank_{sid}"
    assert services["gateway"]["depends_on"]["bank_a1"]["condition"] == "service_healthy"
