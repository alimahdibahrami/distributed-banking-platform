#!/usr/bin/env python3
"""Assignment 2 bonus: custom Docker Compose replica self-healing controller.

Only the four explicitly allowed banking replica SERVICES can be recreated.
Never changes PostgreSQL, Coordinator, Gateway, other Compose projects or volumes.
Container automatic restart policies MUST be disabled in compose.yaml.
"""
from __future__ import annotations

import argparse
import fcntl
import json
import logging
import os
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol

LOGGER = logging.getLogger("self_healing")
PROJECT = "distributed-banking"
SERVICES = ("bank_a1", "bank_a2", "bank_b1", "bank_b2")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def atomic_json_write(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".selfheal-", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as out:
            json.dump(data, out, indent=2, sort_keys=True)
            out.write("\n")
            out.flush()
            os.fsync(out.fileno())
        os.replace(tmp, path)
        dirfd = os.open(str(path.parent), os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(dirfd)
        finally:
            os.close(dirfd)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def load_state(path: Path) -> dict:
    if not path.exists():
        return {"version": 1, "services": {}, "events": []}
    doc = json.loads(path.read_text(encoding="utf-8"))
    if doc.get("version") != 1 or not isinstance(doc.get("services"), dict):
        raise RuntimeError("Invalid self-healing durable state: refusing unsafe recovery")
    if not isinstance(doc.get("events"), list):
        raise RuntimeError("Invalid self-healing event log")
    return doc


@dataclass(frozen=True)
class Observation:
    container_id: str | None
    status: str
    health: str
    age_seconds: float

    @property
    def healthy(self) -> bool:
        return self.status == "running" and self.health == "healthy"


class Backend(Protocol):
    def observe(self, service: str) -> Observation: ...
    def replace(self, service: str) -> None: ...


class DockerComposeBackend:
    def __init__(self, directory: Path):
        self.directory = directory

    def run(self, *args: str, timeout: int = 120) -> str:
        p = subprocess.run(args, cwd=self.directory, capture_output=True, text=True,
                           timeout=timeout, check=False)
        if p.returncode != 0:
            detail = ((p.stderr or p.stdout).strip())[-1200:]
            raise RuntimeError(f"{args[0]} command failed ({p.returncode}): {detail}")
        return p.stdout.strip()

    def assert_isolated_config(self) -> None:
        if not (self.directory / "compose.yaml").is_file():
            raise RuntimeError("compose.yaml missing")
        doc = json.loads(self.run("docker", "compose", "-f", "compose.yaml", "config", "--format", "json"))
        if doc.get("name") != PROJECT:
            raise RuntimeError("Unexpected Compose project; refusing to operate")
        for svc in SERVICES:
            config = doc.get("services", {}).get(svc, {})
            if config.get("restart", "no") not in ("no", ""):
                raise RuntimeError(f"Automatic Docker restart forbidden on {svc}")
        # No need to print resolved compose: could contain database credentials.

    def observe(self, service: str) -> Observation:
        if service not in SERVICES:
            raise ValueError("Only banking replicas can be inspected")
        identifier = self.run("docker", "compose", "-f", "compose.yaml", "ps", "-a", "-q", service)
        if not identifier:
            return Observation(None, "absent", "absent", float("inf"))
        identifiers = identifier.splitlines()
        if len(identifiers) != 1:
            raise RuntimeError(f"Ambiguous containers for {service}; refusing to heal")
        raw = self.run("docker", "inspect", identifiers[0])
        info = json.loads(raw)[0]
        labels = info.get("Config", {}).get("Labels", {})
        if (labels.get("com.docker.compose.project") != PROJECT or
                labels.get("com.docker.compose.service") != service):
            raise RuntimeError("Container label mismatch; refusing cross-project action")
        state = info.get("State", {})
        health = state.get("Health", {}).get("Status", "none")
        status = state.get("Status", "unknown")
        age = float("inf")
        try:
            started = datetime.fromisoformat(state["StartedAt"].replace("Z", "+00:00"))
            age = max(0.0, (datetime.now(timezone.utc) - started).total_seconds())
        except (KeyError, ValueError, OverflowError):
            pass
        return Observation(info.get("Id", identifiers[0]), status, health, age)

    def replace(self, service: str) -> None:
        if service not in SERVICES:
            raise ValueError("Only banking replicas can be replaced")
        # Docker Compose uses a stable service identity/alias. Force recreate is
        # scoped to ONE permitted service; --no-deps excludes DB/Gateway/Coordinator.
        self.run("docker", "compose", "-f", "compose.yaml", "up", "-d",
                 "--no-deps", "--force-recreate", "--wait", "--wait-timeout", "100",
                 service, timeout=145)
        if not self.observe(service).healthy:
            raise RuntimeError(f"Replacement {service} did not reach healthy state")


class Reconciler:
    def __init__(self, backend: Backend, state_path: Path, *, failures: int = 2,
                 cooldown: float = 50, start_grace: float = 45,
                 now=time.time, dry_run=False):
        if failures < 1 or cooldown < 0 or start_grace < 0:
            raise ValueError("Invalid controller policy")
        self.backend = backend
        self.state_path = state_path
        self.failures = failures
        self.cooldown = cooldown
        self.start_grace = start_grace
        self.now = now
        self.dry_run = dry_run
        self.state = load_state(state_path)

    def _event(self, service: str, kind: str, **details) -> None:
        event = {"at": utc_now(), "service": service, "event": kind, **details}
        LOGGER.info("selfheal.%s service=%s details=%s", kind, service, details)
        self.state.setdefault("events", []).append(event)
        self.state["events"] = self.state["events"][-150:]

    def once(self) -> None:
        for service in SERVICES:
            self._once_service(service)
        if not self.dry_run:
            atomic_json_write(self.state_path, self.state)

    def _once_service(self, service: str) -> None:
        obs = self.backend.observe(service)
        old = self.state["services"].setdefault(service, {})
        if obs.healthy:
            # Idempotency after a controller crash: inspect the already healthy
            # Compose service instead of recreating it again.
            if old.get("failures") or old.get("repair_started"):
                self._event(service, "recovered", container_id=obs.container_id)
            old["failures"] = 0
            old["repair_started"] = False
            old["last_healthy_id"] = obs.container_id
            return
        if obs.status in ("created", "restarting", "running") and obs.age_seconds < self.start_grace:
            LOGGER.info("selfheal.grace service=%s age_seconds=%s", service, round(obs.age_seconds))
            return
        old["failures"] = int(old.get("failures", 0)) + 1
        if old["failures"] < self.failures:
            self._event(service, "suspected", health=obs.health, status=obs.status)
            return
        if self.now() - float(old.get("last_attempt", 0)) < self.cooldown:
            LOGGER.warning("selfheal.cooldown service=%s", service)
            return
        if self.dry_run:
            self._event(service, "would_replace", status=obs.status, health=obs.health)
            return
        old["last_attempt"] = self.now()
        old["repair_started"] = True
        self._event(service, "repair_started", old_id=obs.container_id)
        # Persist intent BEFORE invoking Docker: a controller crash here is
        # recoverable by re-inspection of Compose's unique service container.
        atomic_json_write(self.state_path, self.state)
        try:
            self.backend.replace(service)
        except Exception as exc:
            self._event(service, "repair_failed", error=type(exc).__name__)
            atomic_json_write(self.state_path, self.state)
            raise
        new = self.backend.observe(service)
        if not new.healthy:
            raise RuntimeError(f"Replacement not healthy: {service}")
        old["repair_started"] = False
        old["failures"] = 0
        old["last_healthy_id"] = new.container_id
        self._event(service, "replaced", old_id=obs.container_id, new_id=new.container_id)
        atomic_json_write(self.state_path, self.state)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--project-dir", default="/opt/distributed-banking-platform")
    ap.add_argument("--state-file", default="/var/lib/distributed-banking-selfheal/state.json")
    ap.add_argument("--interval", type=float, default=5)
    ap.add_argument("--failures", type=int, default=2)
    ap.add_argument("--cooldown", type=float, default=50)
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args(argv)
    if args.interval < 1:
        ap.error("--interval must be at least 1 second")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    backend = DockerComposeBackend(Path(args.project_dir).resolve())
    backend.assert_isolated_config()
    state = Path(args.state_file)
    if args.dry_run:
        # Dry-run has no disk-side effects.
        agent = Reconciler(backend, state, failures=args.failures, cooldown=args.cooldown, dry_run=True)
        agent.once()
        print("SELFHEAL_DRY_RUN_OK")
        return 0
    state.parent.mkdir(parents=True, exist_ok=True)
    with open(state.parent / "controller.lock", "a+") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("Another self-healing controller is already running") from None
        agent = Reconciler(backend, state, failures=args.failures, cooldown=args.cooldown)
        while True:
            try:
                agent.once()
            except Exception:
                LOGGER.exception("selfheal.reconcile_failed")
                if args.once:
                    return 1
            if args.once:
                return 0
            time.sleep(args.interval)


if __name__ == "__main__":
    sys.exit(main())
