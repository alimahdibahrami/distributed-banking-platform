import pytest

from bank_platform.common.validation import MAX_MONEY, BankingError, LocalOperation, validate_local_operation


@pytest.mark.parametrize("payload,expected", [
    ({"action": "balance", "account": "A0001"}, LocalOperation("balance", "A0001", None)),
    ({"action": "deposit", "account": "A1001", "amount": 50}, LocalOperation("deposit", "A1001", 50)),
    ({"action": "withdraw", "account": "A2000", "amount": MAX_MONEY}, LocalOperation("withdraw", "A2000", MAX_MONEY)),
])
def test_valid_local_operations(payload, expected):
    assert validate_local_operation(payload) == expected


@pytest.mark.parametrize("payload,code", [
    ([], "INVALID_REQUEST"),
    ({"action": "balance"}, "INVALID_REQUEST"),
    ({"action": "balance", "account": "A0001", "amount": 2}, "INVALID_REQUEST"),
    ({"action": "transfer", "account": "A0001"}, "UNSUPPORTED_ACTION"),
    ({"action": "deposit", "account": "A0001", "amount": True}, "INVALID_AMOUNT"),
    ({"action": "withdraw", "account": "A0001", "amount": 0}, "INVALID_AMOUNT"),
    ({"action": "withdraw", "account": "A0001", "amount": -1}, "INVALID_AMOUNT"),
    ({"action": "withdraw", "account": "A0001", "amount": MAX_MONEY + 1}, "INVALID_AMOUNT"),
    ({"action": "deposit", "account": "  ", "amount": 2}, "INVALID_REQUEST"),
])
def test_invalid_local_operations(payload, code):
    with pytest.raises(BankingError) as exc:
        validate_local_operation(payload)
    assert exc.value.code == code
