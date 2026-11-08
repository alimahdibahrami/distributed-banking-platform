"""Internal opt-in migration epoch contract. Public API never accepts epochs."""
from __future__ import annotations

import os

from bank_platform.common.validation import BankingError


def enabled() -> bool:
    return os.getenv("BANK_EPOCH_PROPAGATION", "0") == "1"


def outgoing_epoch(version: int) -> dict[str, int]:
    """Only trusted routing/coordinator layers add this field."""
    if type(version) is not int or version < 1:
        raise ValueError("Invalid map epoch")
    return {"owner_epoch": version} if enabled() else {}


def strip_internal_epoch(body: object) -> tuple[object, int | None]:
    """Remove a trusted internal epoch before legacy strict schema validation.

    Unprotected old request contracts remain unchanged when the feature is off.
    An attacker cannot set this through the public Gateway, which applies its
    strict public validator before constructing an internal command.
    """
    if not isinstance(body, dict) or "owner_epoch" not in body:
        return body, None
    if not enabled():
        raise BankingError("INVALID_REQUEST", "Epoch propagation is disabled", 400)
    epoch = body["owner_epoch"]
    if type(epoch) is not int or epoch < 1:
        raise BankingError("INVALID_OWNER_EPOCH", "Expected positive integer epoch", 400)
    cleaned = dict(body)
    del cleaned["owner_epoch"]
    return cleaned, epoch


def stale_epoch_error(exc: Exception) -> BankingError | None:
    """Map only the explicit database fence error, never generic SQL errors."""
    if getattr(exc, "sqlstate", None) == "P0001" and "STALE_OWNER_EPOCH" in str(exc):
        return BankingError("STALE_OWNER_EPOCH", "Write rejected by durable shard ownership fence", 409)
    return None
