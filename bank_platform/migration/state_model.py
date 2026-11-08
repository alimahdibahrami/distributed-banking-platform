"""Pure crash-recovery state model for future Phase 10 protocol.

IMPORTANT: This is a model + test scaffold, not a live migrator.
Actual implementation must persist transitions atomically before side effects.
"""
from __future__ import annotations
from dataclasses import dataclass, replace
from enum import Enum


class Phase(str, Enum):
    PLANNED = "PLANNED"
    SNAPSHOTTING = "SNAPSHOTTING"
    CATCHING_UP = "CATCHING_UP"
    FENCED = "FENCED"
    ACTIVATED = "ACTIVATED"
    VERIFYING = "VERIFYING"
    COMPLETED = "COMPLETED"


NEXT = {
    Phase.PLANNED: Phase.SNAPSHOTTING,
    Phase.SNAPSHOTTING: Phase.CATCHING_UP,
    Phase.CATCHING_UP: Phase.FENCED,
    Phase.FENCED: Phase.ACTIVATED,
    Phase.ACTIVATED: Phase.VERIFYING,
    Phase.VERIFYING: Phase.COMPLETED,
}


@dataclass(frozen=True)
class MigrationState:
    phase: Phase = Phase.PLANNED
    old_owner: str = "B"
    new_owner: str = "C"
    first: str = "A1501"
    last: str = "A2000"
    source_epoch: int = 1
    target_epoch: int = 2
    checkpoint: int = 0
    committed_cutover: bool = False
    verified: bool = False

    def advance(self, target: Phase, *, checkpoint: int | None = None,
                inflight_prepared: int = 0, source_fenced: bool = False,
                target_caught_up: bool = False, new_owner_active: bool = False,
                verification_passed: bool = False) -> "MigrationState":
        if NEXT.get(self.phase) != target:
            raise ValueError("Migration steps cannot be skipped or reversed")
        if self.target_epoch <= self.source_epoch:
            raise ValueError("Target epoch must increase monotonically")
        cp = self.checkpoint if checkpoint is None else checkpoint
        if cp < self.checkpoint:
            raise ValueError("Durable change-log checkpoint must be monotonic")
        if target == Phase.FENCED and (inflight_prepared or not target_caught_up or not source_fenced):
            raise ValueError("Source must be fenced and 2PC drained before cutover")
        if target == Phase.ACTIVATED and (not source_fenced or not target_caught_up or not new_owner_active):
            raise ValueError("Never activate two writable owners")
        if target == Phase.COMPLETED and not verification_passed:
            raise ValueError("Never remove old data before verified completion")
        return replace(self, phase=target, checkpoint=cp,
                       committed_cutover=self.committed_cutover or target == Phase.ACTIVATED,
                       verified=self.verified or verification_passed)
