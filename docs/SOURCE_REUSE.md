# Assignment 1 -> Assignment 2 reuse record

- `bank_platform/common/validation.py`: adapted A1's `_validate()` and
  `BankingError` into a pure validator. Local operations only; transfer is pending.
- `client/main.py`: adapted A1's HTTPX CLI with a Gateway health command. In this
  phase balance/deposit/withdraw/load are intentionally not functional.
- `Dockerfile`: retains the A1 pattern (non-root process and read-only runtime),
  but A1's SQLite directories and single-bank launch have been removed.
- `tests/`: new focused tests; A1's old SQLite ACID tests are *not* claimed
  to validate PostgreSQL or distributed correctness.
- The A1 database, logs, volumes, restart/upgrade scripts, and server file
  are not bundled or modified. This new Compose project has different names.
