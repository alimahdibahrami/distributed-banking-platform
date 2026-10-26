"""Assignment 1 CLI adapted for Gateway-only communication (Phase 1 scaffold)."""
from __future__ import annotations

import argparse
import asyncio
import json
from collections import Counter

import httpx


def display(response: httpx.Response) -> dict:
    try:
        value = response.json()
    except ValueError:
        value = {"raw_response": response.text[:400]}
    print(json.dumps({"http_status": response.status_code, "response": value}, indent=2, ensure_ascii=False))
    return value


async def load(base_url: str, account: str, amount: int, count: int, parallel: int) -> None:
    semaphore = asyncio.Semaphore(parallel)
    timeout = httpx.Timeout(30.0)
    async with httpx.AsyncClient(base_url=base_url, timeout=timeout) as client:
        async def one(index: int) -> tuple[int, str]:
            async with semaphore:
                try:
                    resp = await client.post(
                        "/api/v1/operations",
                        json={"action": "withdraw", "account": account, "amount": amount},
                    )
                    data = resp.json()
                    return resp.status_code, data.get("error", {}).get("code", "success")
                except (httpx.HTTPError, ValueError) as exc:
                    return 0, type(exc).__name__

        before = await client.post("/api/v1/operations", json={"action": "balance", "account": account})
        results = await asyncio.gather(*(one(i) for i in range(count)))
        after = await client.post("/api/v1/operations", json={"action": "balance", "account": account})
    summary = {
        "start_balance": before.json().get("balance") if before.status_code == 200 else None,
        "requests": count,
        "parallel": parallel,
        "withdraw_amount": amount,
        "result_counts": {f"{code}/{label}": n for (code, label), n in Counter(results).items()},
        "final_balance": after.json().get("balance") if after.status_code == 200 else None,
    }
    initial, final = summary["start_balance"], summary["final_balance"]
    succeeded = sum(1 for code, label in results if code == 200 and label == "success")
    if isinstance(initial, int) and isinstance(final, int):
        summary["accounting_correct"] = initial - (succeeded * amount) == final and final >= 0
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    if summary.get("accounting_correct") is not True:
        raise SystemExit("FAIL: accounting result could not be verified")


def main() -> None:
    parser = argparse.ArgumentParser(description="Distributed Banking Gateway client")
    parser.add_argument("--server", required=True, help="Example: http://SERVER_VM_IP:8000")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("health", help="Check Gateway liveness (not banking readiness)")
    for action in ("balance", "deposit", "withdraw"):
        p = sub.add_parser(action)
        p.add_argument("account")
        if action != "balance":
            p.add_argument("amount", type=int)
    transfer = sub.add_parser("transfer", help="Transfer within a shard; cross-shard awaits 2PC")
    transfer.add_argument("source")
    transfer.add_argument("destination")
    transfer.add_argument("amount", type=int)
    raw = sub.add_parser("raw", help="Send arbitrary body for malformed/error tests")
    raw.add_argument("body", help="Literal body; e.g. '{broken' or '{\"action\":\"transfer\"}'")
    load_parser = sub.add_parser("load", help="Concurrent withdraw test")
    load_parser.add_argument("--account", default="A1001")
    load_parser.add_argument("--amount", type=int, default=100)
    load_parser.add_argument("--count", type=int, default=20)
    load_parser.add_argument("--parallel", type=int, default=5)
    args = parser.parse_args()
    base = args.server.rstrip("/")
    if args.command == "load":
        if args.count < 1 or args.parallel < 1 or args.amount < 1:
            parser.error("count, parallel and amount must be positive")
        asyncio.run(load(base, args.account, args.amount, args.count, args.parallel))
        return
    with httpx.Client(base_url=base, timeout=30) as client:
        if args.command == "health":
            resp = client.get("/health")
        elif args.command == "raw":
            resp = client.post("/api/v1/operations", content=args.body.encode(), headers={"content-type": "application/json"})
        elif args.command == "transfer":
            resp = client.post("/api/v1/operations", json={"action": "transfer",
                               "from_account": args.source, "to_account": args.destination,
                               "amount": args.amount})
        else:
            payload = {"action": args.command, "account": args.account}
            if args.command != "balance":
                payload["amount"] = args.amount
            resp = client.post("/api/v1/operations", json=payload)
        display(resp)
        if resp.status_code >= 500:
            raise SystemExit(1)


if __name__ == "__main__":
    main()
