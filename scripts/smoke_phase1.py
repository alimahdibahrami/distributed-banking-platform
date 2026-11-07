"""Minimal external smoke test against the gateway on host loopback."""
from __future__ import annotations

import argparse
import json
import urllib.error
import urllib.request


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:8001")
    args = parser.parse_args()
    base = args.url.rstrip("/")
    with urllib.request.urlopen(base + "/health", timeout=5) as response:
        health = json.load(response)
        assert response.status == 200
        assert health == {"status": "alive", "service": "gateway"}
    request = urllib.request.Request(
        base + "/api/v1/operations",
        data=b'{"action":"balance","account":"A0001"}',
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        urllib.request.urlopen(request, timeout=5)
    except urllib.error.HTTPError as response:
        assert response.code == 501
        body = json.load(response)
        assert body["error"]["code"] == "NOT_IMPLEMENTED"
    else:
        raise AssertionError("Banking endpoint must not report success in Phase 1")
    print("PHASE 1 SMOKE PASS: gateway alive; banking correctly not implemented")


if __name__ == "__main__":
    main()
