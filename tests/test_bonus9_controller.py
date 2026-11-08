"""Pure unit tests; no Docker daemon or real bank balances required."""
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
from self_healing_controller import Observation, Reconciler, SERVICES, load_state


class FakeBackend:
    def __init__(self):
        self.items = {s: Observation("old-"+s, "running", "healthy", 300) for s in SERVICES}
        self.replacements = []

    def observe(self, service):
        return self.items[service]

    def replace(self, service):
        self.replacements.append(service)
        self.items[service] = Observation("new-"+service, "running", "healthy", 0)


def test_replaces_only_failed_service_and_rejoins(tmp_path):
    b = FakeBackend()
    b.items["bank_a1"] = Observation("old-a1", "exited", "none", 400)
    s = tmp_path / "state.json"
    r = Reconciler(b, s, failures=2, cooldown=0, now=lambda: 1000)
    r.once()
    assert not b.replacements
    r.once()
    assert b.replacements == ["bank_a1"]
    assert b.observe("bank_a1").healthy
    assert load_state(s)["services"]["bank_a1"]["failures"] == 0


def test_controller_crash_after_replacement_does_not_duplicate(tmp_path):
    b = FakeBackend()
    b.items["bank_b2"] = Observation("old-b2", "exited", "none", 400)
    state = tmp_path / "state.json"
    r = Reconciler(b, state, failures=1, cooldown=0, now=lambda: 1000)
    r.once()
    assert b.replacements == ["bank_b2"]
    # Simulate process death/restart, with prior durable state on disk.
    restarted = Reconciler(b, state, failures=1, cooldown=0, now=lambda: 1050)
    restarted.once()
    assert b.replacements == ["bank_b2"]


def test_dry_run_never_mutates_anything(tmp_path):
    b = FakeBackend()
    b.items["bank_a2"] = Observation(None, "absent", "absent", float("inf"))
    f = tmp_path / "state.json"
    Reconciler(b, f, failures=1, cooldown=0, now=lambda: 1000, dry_run=True).once()
    assert not b.replacements and not f.exists()


def test_transient_failure_and_grace_no_replacement(tmp_path):
    b = FakeBackend()
    b.items["bank_b1"] = Observation("new-1", "running", "starting", 8)
    r = Reconciler(b, tmp_path / "state", failures=2, cooldown=0)
    r.once()
    r.once()
    assert not b.replacements
    b.items["bank_b1"] = Observation("new-1", "running", "healthy", 40)
    r.once()
    assert not b.replacements


def test_cooldown_stops_restart_loop(tmp_path):
    b = FakeBackend()
    b.items["bank_a1"] = Observation("a1", "exited", "none", 999)
    r = Reconciler(b, tmp_path / "state", failures=1, cooldown=50, now=lambda: 1010)
    r.state["services"]["bank_a1"] = {"last_attempt": 1000}
    r.once()
    assert not b.replacements


def test_atomic_state_loaded_after_restart(tmp_path):
    b = FakeBackend()
    s = tmp_path / "nested" / "state.json"
    Reconciler(b, s).once()
    doc = load_state(s)
    assert doc["version"] == 1
    assert set(doc["services"]) == set(SERVICES)
