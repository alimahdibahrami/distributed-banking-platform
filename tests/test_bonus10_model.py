"""Safety properties of the *model* only, not a real online migration test."""
import sys
from pathlib import Path
import pytest
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from bank_platform.migration.state_model import MigrationState, Phase
sys.path.insert(0, str(ROOT / 'scripts'))
from phase10_preflight import inspect_source


def test_sequence_never_skips_phases():
    s = MigrationState()
    with pytest.raises(ValueError):
        s.advance(Phase.FENCED, source_fenced=True, target_caught_up=True)
    assert s.advance(Phase.SNAPSHOTTING).phase == Phase.SNAPSHOTTING


def test_epoch_must_increase():
    with pytest.raises(ValueError):
        MigrationState(target_epoch=1).advance(Phase.SNAPSHOTTING)


def test_must_drain_2pc_and_fence_before_cutover():
    s = MigrationState().advance(Phase.SNAPSHOTTING).advance(Phase.CATCHING_UP)
    with pytest.raises(ValueError):
        s.advance(Phase.FENCED, inflight_prepared=1, source_fenced=True, target_caught_up=True)
    with pytest.raises(ValueError):
        s.advance(Phase.FENCED, source_fenced=False, target_caught_up=True)
    assert s.advance(Phase.FENCED, source_fenced=True, target_caught_up=True).phase == Phase.FENCED


def test_epoch_activated_only_after_fence():
    s = MigrationState(phase=Phase.FENCED)
    with pytest.raises(ValueError):
        s.advance(Phase.ACTIVATED, source_fenced=False, target_caught_up=True, new_owner_active=True)
    s = s.advance(Phase.ACTIVATED, source_fenced=True, target_caught_up=True, new_owner_active=True)
    assert s.committed_cutover


def test_checkpoint_monotonic():
    s = MigrationState(phase=Phase.SNAPSHOTTING, checkpoint=99)
    with pytest.raises(ValueError):
        s.advance(Phase.CATCHING_UP, checkpoint=98)
    assert s.advance(Phase.CATCHING_UP, checkpoint=99).checkpoint == 99


def test_cleanup_requires_verification():
    s = MigrationState(phase=Phase.VERIFYING, committed_cutover=True)
    with pytest.raises(ValueError):
        s.advance(Phase.COMPLETED)
    assert s.advance(Phase.COMPLETED, verification_passed=True).verified


def test_source_preflight_recognizes_current_baseline():
    report = inspect_source(ROOT.parent.parent / 'reference') if (ROOT.parent.parent / 'reference').exists() else None
    # Standalone patch does not contain baseline; on full repo it will.
    if report:
        assert report['baseline_valid'] is True
        assert report['shard_c_defined'] is False
