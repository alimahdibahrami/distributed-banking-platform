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


@dataclass(frozen=True)
class TransferOperation:
    source: str
    destination: str
    amount: int


def validate_transfer(raw: object) -> TransferOperation:
    """Validate the local-transfer schema without assuming both accounts share a shard."""
    if type(raw) is not dict or set(raw) != {"action", "from_account", "to_account", "amount"}:
        raise BankingError("INVALID_REQUEST", "Transfer requires action, from_account, to_account and amount", 400)
    if raw["action"] != "transfer":
        raise BankingError("UNSUPPORTED_ACTION", "Expected transfer action", 400)
    from bank_platform.common.shard_map import account_number
    for key in ("from_account", "to_account"):
        try:
            account_number(raw[key])
        except ValueError:
            raise BankingError("INVALID_ACCOUNT", "Account ID must be A0001..A2000", 400) from None
    if raw["from_account"] == raw["to_account"]:
        raise BankingError("SAME_ACCOUNT", "Transfer source and destination must differ", 400)
    amount = raw["amount"]
    if type(amount) is not int or amount <= 0 or amount > MAX_MONEY:
        raise BankingError("INVALID_AMOUNT", "amount must be a positive 64-bit integer", 400)
    return TransferOperation(raw["from_account"], raw["to_account"], amount)
