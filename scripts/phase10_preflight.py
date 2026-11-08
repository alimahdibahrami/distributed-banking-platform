#!/usr/bin/env python3
"""READ-ONLY Phase 10 audit. This script NEVER migrates accounts."""
from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path

BLOCKERS = (
    "ShardMap loader hardcodes exactly two shards and fixed A/B ranges",
    "PostgreSQL shard identities and constraints hardcode B owning A1001..A2000",
    "Coordinator journal uses only a_ack/b_ack; no persisted participant epochs",
    "No committed-order CDC, idempotent replay, or durable checkpoints",
    "No shard-enforced epoch fence and crash-consistent cutover protocol",
    "No Shard C database and two application replicas in the stable Compose topology",
)


def inspect_source(root: Path) -> dict:
    required = ["compose.yaml", "config/shard_map.json", "bank_platform/common/shard_map.py",
                "bank_platform/coordinator/engine.py", "bank_platform/shard/storage.py",
                "bank_platform/shard/twopc.py", "database/shard_b.sql"]
    missing = [s for s in required if not (root / s).is_file()]
    if missing:
        return {"baseline_valid": False, "missing": missing, "blockers": list(BLOCKERS)}
    shards = json.loads((root / "config/shard_map.json").read_text(encoding="utf-8"))
    ranges = [(s.get("id"), s.get("first"), s.get("last")) for s in shards["shards"]]
    expected = [("A", "A0001", "A1000"), ("B", "A1001", "A2000")]
    compose = (root / "compose.yaml").read_text(encoding="utf-8")
    return {
        "baseline_valid": ranges == expected and "name: distributed-banking" in compose,
        "shard_map_version": shards.get("version"),
        "ranges": ranges,
        "shard_c_defined": "shard_c_db:" in compose and "bank_c1:" in compose and "bank_c2:" in compose,
        "blockers": list(BLOCKERS),
    }


def docker_readonly(root: Path, *argv: str, timeout: int = 20) -> str:
    proc = subprocess.run(["docker", "compose", "-f", "compose.yaml", *argv],
                          cwd=root, text=True, capture_output=True, timeout=timeout)
    if proc.returncode:
        raise RuntimeError("Read-only Docker check failed: " + (proc.stderr or "")[-400:])
    return proc.stdout.strip()


def live_audit(root: Path) -> dict:
    result: dict = {}
    for shard in ("a", "b"):
        query = "SELECT count(*), coalesce(sum(balance),0) FROM accounts"
        raw = docker_readonly(root, "exec", "-T", f"shard_{shard}_db",
                              "psql", "-U", "bank", "-d", f"bank_{shard}", "-At", "-c", query)
        count, balance = raw.split("|", 1)
        prepared = docker_readonly(root, "exec", "-T", f"shard_{shard}_db",
                                  "psql", "-U", "bank", "-d", f"bank_{shard}", "-At", "-c",
                                  "SELECT count(*) FROM pg_prepared_xacts WHERE database=current_database()")
        result[f"shard_{shard.upper()}"] = {"accounts": int(count), "sum": int(balance),
                                             "prepared": int(prepared)}
    # No writes to coordinator journal; use SQLite URI read-only.
    query = ("import sqlite3; p='file:/app/state/coordinator.sqlite3?mode=ro'; "
             "db=sqlite3.connect(p,uri=True); "
             "print(db.execute(\"SELECT count(*) FROM transactions "
             "WHERE state IN ('NEW','COMMIT','ABORT')\").fetchone()[0])")
    pending = docker_readonly(root, "exec", "-T", "coordinator", "python", "-c", query)
    result["coordinator_pending"] = int(pending)
    result["prepared_zero"] = all(result[f"shard_{s}"]["prepared"] == 0 for s in ("A", "B"))
    result["historical_transactions_terminal"] = int(pending) == 0
    return result


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("."))
    parser.add_argument("--live", action="store_true", help="Queries Docker/DB read-only; no changes")
    args = parser.parse_args(argv)
    root = args.root.resolve()
    report = inspect_source(root)
    if args.live:
        report["live"] = live_audit(root)
    print(json.dumps(report, indent=2, ensure_ascii=False))
    print("PHASE10_BASELINE_AUDIT_DONE__MIGRATION_NOT_ENABLED")
    return 0 if report["baseline_valid"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
