#!/usr/bin/env python3
"""Live bonus-9 acceptance. --inject explicitly STOPS only bank_a1."""
from __future__ import annotations

import argparse
import json
import subprocess
import time
import urllib.error
import urllib.request
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

SERVICE = "bank_a1"
GATEWAY = "http://127.0.0.1:8001"


def cmd(*args, timeout=25):
    p = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
    if p.returncode:
        raise RuntimeError(f"command failed {args[:4]}: {(p.stderr or p.stdout)[-500:]}")
    return p.stdout.strip()


def fetch(path, timeout=5):
    with urllib.request.urlopen(GATEWAY + path, timeout=timeout) as response:
        if response.status != 200:
            raise AssertionError("Unexpected HTTP response")
        return json.load(response)


def read_once(_):
    data = fetch("/api/v1/accounts/A0701/balance")
    if data.get("account") != "A0701" or data.get("shard") != "A" or data.get("replica") not in ("A1", "A2"):
        raise AssertionError(f"Wrong routing: {data}")
    return data["replica"]


def prepared_zero():
    for shard in "ab":
        output = cmd("docker", "compose", "exec", "-T", f"shard_{shard}_db",
                     "psql", "-U", "bank", "-d", f"bank_{shard}", "-Atc",
                     "SELECT count(*) FROM pg_prepared_xacts WHERE database=current_database()")
        if output != "0":
            raise RuntimeError(f"Shard {shard.upper()} has prepared transactions; aborting test")


def container_id():
    return cmd("docker", "compose", "ps", "-a", "-q", SERVICE)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--inject", action="store_true", help="stop one banking replica under load")
    ap.add_argument("--evidence-dir", type=Path, default=Path("evidence/bonus9"))
    args = ap.parse_args()
    if not args.inject:
        print("READ_ONLY: pass --inject to run the one-replica disruption test")
        print("Gateway:", fetch("/ready"))
        prepared_zero()
        print("BONUS9_READ_ONLY_PREFLIGHT_PASS")
        return
    if cmd("systemctl", "is-active", "distributed-banking-selfheal") != "active":
        raise RuntimeError("Controller systemd service is not active")
    prepared_zero()
    ready = fetch("/ready")
    if ready.get("healthy_replicas") != {"A": 2, "B": 2}:
        raise RuntimeError("Preflight requires both replicas per shard to be healthy")
    old = container_id()
    if not old:
        raise RuntimeError("Target service does not exist")
    results = []
    errors = []
    from threading import Event, Thread
    finish = Event()

    def traffic():
        # Continuous concurrent traffic throughout the outage and repair.
        while not finish.is_set():
            try:
                results.append(read_once(0))
            except Exception as exc:
                errors.append(type(exc).__name__)
            time.sleep(0.04)

    workers = [Thread(target=traffic, daemon=True) for _ in range(4)]
    for worker in workers:
        worker.start()
    time.sleep(0.4)
    # STOP only bank_a1; never call down, prune, kill on any other service.
    cmd("docker", "stop", old, timeout=35)
    deadline = time.monotonic() + 180
    restored = False
    new = None
    try:
        while time.monotonic() < deadline:
            time.sleep(3)
            new = container_id()
            if new and new != old:
                inspect = json.loads(cmd("docker", "inspect", new))[0]
                if inspect["State"].get("Health", {}).get("Status") == "healthy":
                    restored = True
                    break
    finally:
        finish.set()
        for worker in workers:
            worker.join(timeout=10)
    if any(worker.is_alive() for worker in workers):
        raise RuntimeError("Reader workers did not terminate")
    # Gateway health poll every 2 sec; verify A1 has rejoined routing.
    rejoined = False
    if restored:
        for _ in range(20):
            try:
                count = Counter(read_once(i) for i in range(12))
                if count["A1"] and count["A2"]:
                    rejoined = True
                    break
            except Exception:
                pass
            time.sleep(2)
    report = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "old_container": old, "new_container": new,
        "reads": len(results), "errors": errors, "distribution": dict(Counter(results)),
        "restored": restored, "round_robin_rejoined": rejoined,
    }
    args.evidence_dir.mkdir(parents=True, exist_ok=True)
    path = args.evidence_dir / ("selfheal-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + ".json")
    path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))
    print("Evidence:", path)
    if errors or not restored or not rejoined or len(results) < 20:
        raise SystemExit("BONUS9_FAIL; inspect evidence and recover ONLY the named replica")
    prepared_zero()
    print("BONUS9_AUTONOMOUS_RECOVERY_PASS")


if __name__ == "__main__":
    main()
