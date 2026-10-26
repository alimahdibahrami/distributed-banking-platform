#!/usr/bin/env python3
"""Live Phase 4 checks against localhost Gateway. No database reset/delete.

Requires Phase 4 deployed with four healthy replicas. Uses accounts A0701,
A0702, A1701, A1702. Tests are balance-neutral on full success; if any
request has uncertain status, it STOPS rather than applying blind retries.
"""
from __future__ import annotations

import concurrent.futures
import json
import urllib.error
import urllib.request
from collections import Counter

BASE = "http://127.0.0.1:8001"


def call(payload: dict) -> tuple[int, dict]:
    body = json.dumps(payload).encode()
    request = urllib.request.Request(BASE + "/api/v1/operations", data=body,
                                     headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as exc:
        return exc.code, json.load(exc)


def balance(account: str) -> int:
    code, result = call({"action": "balance", "account": account})
    assert code == 200, f"balance {account} failed: {code} {result}"
    assert result.get("account") == account and type(result.get("balance")) is int, result
    return result["balance"]


def operation(action: str, account: str, amount: int) -> dict:
    code, result = call({"action": action, "account": account, "amount": amount})
    assert code == 200, f"{action} {account} returned {code}: {result}; STOP -- do not blindly retry"
    assert result.get("account") == account and result.get("action") == action, result
    return result


def transfer(source: str, destination: str, amount: int) -> dict:
    code, result = call({"action": "transfer", "from_account": source,
                         "to_account": destination, "amount": amount})
    assert code == 200, f"local transfer {source} -> {destination} returned {code}: {result}; STOP"
    assert result.get("from_account") == source and result.get("to_account") == destination, result
    return result


def concurrent_run(tasks, label: str):
    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
        results = list(executor.map(lambda job: job(), tasks))
    replicas = Counter(r["replica"] for r in results)
    print(f"PASS: {label} ({len(results)} successful writes), replica distribution={dict(replicas)}")
    return replicas


def check() -> None:
    initial = {a: balance(a) for a in ("A0701", "A0702", "A1701", "A1702")}
    print("Initial balances:", initial)

    for shard, account in (("A", "A0701"), ("B", "A1701")):
        before = balance(account)
        code, result = call({"action": "withdraw", "account": account, "amount": before + 1})
        assert code == 409 and result["error"]["code"] == "INSUFFICIENT_FUNDS", result
        assert balance(account) == before, f"Insufficient withdraw changed balance of {account}"
        print(f"PASS: rejected insufficient withdrawal on shard {shard}")

    for shard, account in (("A", "A0701"), ("B", "A1701")):
        # Deposits/withdrawals run against one account through different replicas.
        jobs = ([lambda a=account: operation("deposit", a, 2) for _ in range(30)]
                + [lambda a=account: operation("withdraw", a, 2) for _ in range(30)])
        replicas = concurrent_run(jobs, f"concurrent mutations in shard {shard}")
        assert len(replicas) == 2 and set(replicas) == {shard + "1", shard + "2"}, replicas
        assert balance(account) == initial[account], f"Lost update on {account}"
        print(f"PASS: no lost updates for {account}")

    for shard, source, destination in (("A", "A0701", "A0702"), ("B", "A1701", "A1702")):
        total_before = balance(source) + balance(destination)
        before = (balance(source), balance(destination))
        forward = transfer(source, destination, 10)
        assert (balance(source), balance(destination)) == (before[0] - 10, before[1] + 10)
        transfer(destination, source, 10)
        assert (balance(source), balance(destination)) == before
        assert sum((balance(source), balance(destination))) == total_before
        # Opposing transfers lock accounts in the same global order; no deadlocks.
        jobs = ([lambda x=source,y=destination: transfer(x, y, 1) for _ in range(30)]
                + [lambda x=source,y=destination: transfer(y, x, 1) for _ in range(30)])
        replicas = concurrent_run(jobs, f"overlapping local transfers in shard {shard}")
        assert len(replicas) == 2
        assert (balance(source), balance(destination)) == before
        print(f"PASS: atomic, deadlock-free local transfers preserve sum in shard {shard}")

    # Phase 5 enables cross-shard operations, so verify invalid input is rejected
    # without moving money rather than expecting the former Phase 4 stub 501.
    code, result = call({"action": "transfer", "from_account": "A0701", "to_account": "A1701", "amount": 0})
    assert code == 400 and result.get("error", {}).get("code") == "INVALID_AMOUNT", result
    assert {a: balance(a) for a in initial} == initial, "Test fixture did not end at initial balances"
    print("PASS: invalid cross-shard transfer rejected; all four test balances preserved")
    print("PASS: Phase 4 live verification completed")


if __name__ == "__main__":
    check()
