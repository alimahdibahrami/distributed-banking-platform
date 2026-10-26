"""Phase 3 live Gateway acceptance checks (read-only, no balance changes).

Usage:
    python3 scripts/verify_phase3.py
    python3 scripts/verify_phase3.py --failover

The optional failover test stops ONLY the Phase-3 project's bank_a1 container,
checks traffic continues on bank_a2, and restarts bank_a1 in a finally block.
It never touches Assignment 1 or any database containers/volumes.
"""
from __future__ import annotations

import argparse
import concurrent.futures
import json
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from collections import Counter

BASE = "http://127.0.0.1:8001"


def fetch(path: str, timeout: float = 5) -> tuple[int, dict]:
    try:
        with urllib.request.urlopen(f"{BASE}{path}", timeout=timeout) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as exc:
        return exc.code, json.load(exc)


def compose(*args: str) -> str:
    result = subprocess.run(["docker", "compose", *args], text=True,
                            capture_output=True, check=True)
    return result.stdout.strip()


def wait_until(predicate, *, seconds: int = 50, description: str) -> None:
    deadline = time.monotonic() + seconds
    last_error = ""
    while time.monotonic() < deadline:
        try:
            if predicate():
                return
        except (AssertionError, OSError, ValueError, TimeoutError) as exc:
            last_error = str(exc)
        time.sleep(0.5)
    raise AssertionError(f"Timed out waiting for {description}. Last error: {last_error}")


def ready_counts() -> dict[str, int]:
    code, result = fetch("/ready")
    if code != 200 or not result.get("routing_ready"):
        raise AssertionError(f"Gateway routing not ready: HTTP {code}: {result}")
    return result["healthy_replicas"]


def read_balance(account: str) -> dict:
    code, payload = fetch(f"/api/v1/accounts/{account}/balance")
    assert code == 200, f"Account {account}: HTTP {code}: {payload}"
    assert payload["account"] == account, payload
    return payload


def batch(account: str, count: int = 40) -> Counter:
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        responses = list(executor.map(read_balance, [account] * count))
    return Counter(result["replica"] for result in responses)


def check_base() -> None:
    print("Checking Gateway health and readiness...")
    code, health = fetch("/health")
    assert code == 200 and health.get("status") == "alive", health
    assert health.get("banking_ready") is False, "Writes must not be marked ready in phase 3"
    wait_until(lambda: ready_counts() == {"A": 2, "B": 2}, description="all 4 replicas ready")

    print("Checking owner routing and per-shard Round Robin under parallel reads...")
    counts_a = batch("A0500", 80)
    counts_b = batch("A1500", 80)
    assert set(counts_a) == {"A1", "A2"}, counts_a
    assert set(counts_b) == {"B1", "B2"}, counts_b
    assert abs(counts_a["A1"] - counts_a["A2"]) <= 1, counts_a
    assert abs(counts_b["B1"] - counts_b["B2"]) <= 1, counts_b
    assert read_balance("A0500")["balance"] == 1000
    assert read_balance("A1500")["balance"] == 500
    for invalid in ("A0000", "A2001", "B0001"):
        code, result = fetch(f"/api/v1/accounts/{invalid}/balance")
        assert code == 400 and result["error"]["code"] == "INVALID_ACCOUNT", result
    print(f"PASS: account ownership, validation and replica distribution: A={dict(counts_a)}, B={dict(counts_b)}")


def check_failover() -> None:
    print("Injecting failure into ONLY bank_a1 (reads continue under load)...")
    stopped = False
    running = threading.Event()
    running.set()
    responses: Counter = Counter()
    failures: list[str] = []
    lock = threading.Lock()

    def worker():
        while running.is_set():
            try:
                result = read_balance("A0500")
                with lock:
                    responses[result["replica"]] += 1
            except (AssertionError, OSError, ValueError) as exc:
                with lock:
                    failures.append(str(exc))
            time.sleep(0.04)

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(worker) for _ in range(4)]
            time.sleep(0.5)
            # A normal stop is an injected replica failure for the experiment.
            compose("stop", "bank_a1")
            stopped = True
            wait_until(lambda: ready_counts()["A"] == 1,
                       description="Gateway to mark A1 unavailable")
            time.sleep(2)
            running.clear()
            for future in futures:
                future.result(timeout=10)
        assert responses["A2"] > 0, responses
        assert not failures, f"Errors under injected failure: {failures[:5]}"
        assert batch("A0500", 24) == {"A2": 24}
        assert read_balance("A1500")["replica"] in ("B1", "B2")
        print(f"PASS: A1 failure under concurrent traffic, A2 served {responses['A2']} reads without errors")
    finally:
        running.clear()
        if stopped:
            print("Restoring bank_a1...")
            compose("start", "bank_a1")
            wait_until(lambda: ready_counts() == {"A": 2, "B": 2},
                       description="A1 to rejoin healthy pool")
            assert set(batch("A0500", 20)) == {"A1", "A2"}
            print("PASS: A1 rejoined Round Robin after restart")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--failover", action="store_true",
                        help="Stop and restart bank_a1 while processing concurrent reads")
    args = parser.parse_args()
    check_base()
    if args.failover:
        check_failover()
    print("PASS: Phase 3 live checks completed")


if __name__ == "__main__":
    try:
        main()
    except (AssertionError, OSError, ValueError, subprocess.CalledProcessError) as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        sys.exit(1)
