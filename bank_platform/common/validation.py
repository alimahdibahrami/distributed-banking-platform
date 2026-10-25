"""Reusable local banking request validation adapted from Assignment 1.

No database writes occur in this module. Cross-shard transfer schema will be
added after the coordinator contract is implemented.
"""
from __future__ import annotations

from dataclasses import dataclass

MAX_MONEY = 2**63 - 1


class BankingError(Exception):
    def __init__(self, code: str, message: str, http_status: int):
        super().__init__(message)
        self.code = code
        self.message = message
        self.http_status = http_status


@dataclass(frozen=True)
class LocalOperation:
    action: str
    account: str
    amount: int | None


def validate_local_operation(raw: object) -> LocalOperation:
    """Apply Assignment 1 validation rules to balance/deposit/withdraw."""
    if not isinstance(raw, dict):
        raise BankingError("INVALID_REQUEST", "JSON body must be an object", 400)
    if "action" not in raw or "account" not in raw:
        raise BankingError("INVALID_REQUEST", "Required fields: action and account", 400)
    action = raw["action"]
    if not isinstance(action, str) or action not in ("balance", "deposit", "withdraw"):
        raise BankingError("UNSUPPORTED_ACTION", "Unsupported action", 400)
    account = raw["account"]
    if not isinstance(account, str) or not account.strip():
        raise BankingError("INVALID_REQUEST", "account must be a non-empty string", 400)
    expected = {"action", "account"} if action == "balance" else {"action", "account", "amount"}
    if set(raw) != expected:
        raise BankingError("INVALID_REQUEST", "Missing or unexpected request fields", 400)
    if action == "balance":
        return LocalOperation(action, account, None)
    amount = raw["amount"]
    if type(amount) is not int or amount < 1 or amount > MAX_MONEY:
        raise BankingError("INVALID_AMOUNT", "amount must be a positive 64-bit integer", 400)
    return LocalOperation(action, account, amount)
