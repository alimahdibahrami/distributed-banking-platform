"""Non-destructive live acceptance checks. Run on the Docker Compose host.

Requires: docker compose up -d --build, and healthy database/replica services.
Optional --restart-check validates persistence through a DB container restart.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys


def run(*args: str) -> str:
    result = subprocess.run(["docker", "compose", *args], check=True, capture_output=True, text=True)
    return result.stdout.strip()


def db_scalar(db: str, sql: str) -> str:
    return run("exec", "-T", f"shard_{db.lower()}_db", "psql", "-X", "-U", "bank",
               "-d", f"bank_{db.lower()}", "-t", "-A", "-v", "ON_ERROR_STOP=1", "-c", sql)


def replica_read(replica: str, account: str) -> dict:
    # The probe runs inside the private Compose network; no extra public ports.
    script = ("import json,urllib.request; "
              f"data=json.load(urllib.request.urlopen('http://127.0.0.1:8000/internal/accounts/{account}', timeout=5)); "
              "print(json.dumps(data))")
    return json.loads(run("exec", "-T", f"bank_{replica.lower()}", "python", "-c", script))


def assert_database_rejects_wrong_owner(db: str, wrong_account: str) -> None:
    # The test INSERT is inside an explicit transaction. Even if the CHECK is
    # accidentally missing, a successful probe is rolled back, then fails here.
    statement = f"BEGIN; INSERT INTO accounts(account_id, balance) VALUES ('{wrong_account}', 1); ROLLBACK;"
    cmd = ["docker", "compose", "exec", "-T", f"shard_{db.lower()}_db", "psql",
           "-X", "-U", "bank", "-d", f"bank_{db.lower()}",
           "-v", "ON_ERROR_STOP=1", "-c", statement]
    result = subprocess.run(cmd, capture_output=True, text=True)
    constraint = f"account_id_in_shard_{db.lower()}"
    assert result.returncode != 0 and constraint in result.stderr, (
        f"Wrong-owner INSERT on shard {db} was not rejected by {constraint}: "
        f"return code={result.returncode}, stderr={result.stderr}"
    )


def check() -> None:
    for sid, low, high in (("A", 1, 1000), ("B", 1001, 2000)):
        count = db_scalar(sid, "SELECT count(*) FROM accounts")
        assert count == "1000", f"Shard {sid} count: {count}"
        identities = db_scalar(sid, "SELECT shard_id || ':' || first_account || ':' || last_account FROM shard_metadata")
        assert identities == f"{sid}:{low}:{high}", identities
        offenders = db_scalar(sid, f"SELECT count(*) FROM accounts WHERE substring(account_id from 2)::int NOT BETWEEN {low} AND {high}")
        assert offenders == "0", f"Shard {sid} misplaced accounts: {offenders}"
    assert db_scalar("A", "SELECT count(*) FROM accounts WHERE account_id = 'A1500'") == "0"
    assert db_scalar("B", "SELECT count(*) FROM accounts WHERE account_id = 'A0500'") == "0"
    assert_database_rejects_wrong_owner("A", "A1500")
    assert_database_rejects_wrong_owner("B", "A0500")
    for replica in ("a1", "a2", "b1", "b2"):
        account, expected = ("A0500", 1000) if replica.startswith("a") else ("A1500", 500)
        result = replica_read(replica, account)
        assert result == {"account": account, "balance": expected,
                          "shard": replica[0].upper(), "replica": replica.upper()}, result
    print("PASS: 1000 separate accounts per DB, no overlap, all 4 replicas read correct shared storage")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--restart-check", action="store_true", help="Restart both database containers and recheck persisted data")
    args = parser.parse_args()
    check()
    if args.restart_check:
        run("restart", "shard_a_db", "shard_b_db")
        # `docker compose up -d --wait` waits for dependencies after the restart.
        run("up", "-d", "--wait")
        check()
        print("PASS: accounts survived PostgreSQL container restart")


if __name__ == "__main__":
    try:
        main()
    except (AssertionError, subprocess.CalledProcessError, OSError, ValueError) as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        sys.exit(1)
