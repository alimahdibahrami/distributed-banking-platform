# Phase 7 — Seven mandatory experiments and evidence

**Source of truth:** `Assignment2_Distributed_Banking_Platform.pdf`, §6, exactly seven numbered experiments.
**Status:** test suite implemented and unit-tested; VPS runs **not yet performed**. Do not mark accepted until every experiment has `PASS` in its VPS evidence.

## Matrix against the assignment (no substituted tests)

| PDF # | Instructor experiment | Script(s) | Mandatory evidence |
| --- | --- | --- | --- |
| 1 | Shard distribution | `verify_phase2.py` and `verify_phase3.py` | 1000 accounts/DB, no overlaps, correct Gateway owner responses |
| 2 | Replica load distribution | `verify_phase3.py` | A1/A2 and B1/B2 distribution counts; application `bank.read` logs |
| 3 | Concurrent replica operations | `verify_phase4.py` | 60 deposits/withdrawals per shard, per-replica selection, no lost updates |
| 4 | Replica interruption | `verify_phase3.py --failover` | A1 stopped under read load, A2 serves all reads, A1 restored (DISRUPTIVE) |
| 5 | Local and cross-shard transfers | `verify_phase4.py` + `verify_phase5.py` | Atomic local transfer and true PostgreSQL prepared 2PC, conservation |
| 6 | Interrupted distributed transaction | `verify_phase6.py --crash-tests` | Coordinator crash after durable COMMIT and after first acknowledgement, recovery (DISRUPTIVE) |
| 7 | Concurrent cross-shard transfers | `verify_phase7_concurrent_2pc.py` | Concurrent overlapping accounts, per-UUID final states, exact expected balances, total conservation, idempotent replay |

Notes: Experiment 3 and 5 reuse the Phase 4 verification script; this intentionally reruns balance-neutral operations. Experiment 6 includes the post-first-PREPARE crash as additional evidence. No database reset is used.

## Prerequisites

* Run on the Ubuntu VPS from `/opt/distributed-banking-platform`, with Docker Compose `distributed-banking` already healthy.
* All four bank replicas, both PostgreSQL instances, Coordinator, and Gateway must be online.
* Gateway remains private on `127.0.0.1:8001`; the script does **not** expose a public port.
* No prepared transactions are left from an earlier, unresolved test. Existing `pg_prepared_xacts` cause the suite to STOP rather than remove anything.
* Do not run these tests alongside unrelated balance-mutating experiments on the same test accounts.
* Ensure Assignment 1 continues to use its separate Compose project and port 8000.

## Commands

```bash
cd /opt/distributed-banking-platform
python3 scripts/verify_phase7.py --list

# Safe tests first. These cause normal banking writes (but no deliberate crash).
python3 scripts/verify_phase7.py --experiment 1
python3 scripts/verify_phase7.py --experiment 2
python3 scripts/verify_phase7.py --experiment 3
python3 scripts/verify_phase7.py --experiment 5
python3 scripts/verify_phase7.py --experiment 7

# Fault injections explicitly opt in to container stops.
python3 scripts/verify_phase7.py --experiment 4 --allow-disruptive
python3 scripts/verify_phase7.py --experiment 6 --allow-disruptive

# For final, one-command collection of 7/7 evidence:
python3 scripts/verify_phase7.py --all --allow-disruptive
```

The suite has **no default destructive behavior**. `--list` displays scenarios without accessing the services. Experiments 4 and 6 require `--allow-disruptive`. The test suite does not call `docker compose down`, delete containers, reset data, delete volumes, or restart PostgreSQL.

### Experiment 7 by itself

```bash
python3 scripts/verify_phase7_concurrent_2pc.py \
  --count 16 --workers 3 --evidence evidence/phase7-standalone-experiment7.json
```

Dedicated accounts: `A0901`, `A0902` on A and `A1901`, `A1902` on B. The test submits overlapping transfers with independently generated UUIDv4 IDs from concurrent client workers. It **does not serialize transfers client-side**, nor ignore unknown outcomes. It resolves every original UUID via `GET /api/v1/transactions/<uuid>` before comparing precise expected account balances. A `COMMITTED` move changes its source by `-amount` and destination by `+amount`, while `ABORTED` changes neither. The sum of all four accounts must be constant. It then resubmits exactly **one already-committed UUID** to verify idempotency, and only after complete reconciliation submits distinct reverse transfers to restore the fixture balances.

Under genuine contention, PostgreSQL lock timeouts may result in durable `ABORTED` outcomes; these are **not** reported as commits. The test requires at least two successful COMMITs in distinct account directions. If any transaction remains PENDING/UNKNOWN beyond the bounded query period, or a reverse transfer fails, it **STOPS**, preserves its evidence with transaction IDs, and must not be blindly rerun with new IDs. Check Coordinator and prepared transactions first.

## Artifacts and traceability

Each `verify_phase7.py` execution creates a unique timestamped subdirectory beneath `evidence/phase7/`, including:

* `summary.json`: overall and per-experiment PASS/STOP, UTC start/finish times, durations, step return codes, execution commands.
* `experiment_01` ... `experiment_07`: one `step_*.txt` per subprocess and Docker application log snapshots, Compose states, and Assignment 1 container snapshot.
* `experiment_07/transaction_evidence.json`: every UUID, source/destination, amount, HTTP response, final durable state, initial/after/final balances, and individual reversal UUIDs.
* `initial/` and `final/`: Compose status, selected application logs, and unrelated Assignment 1 status for non-interference.

The `evidence/` directory is `.gitignore`d intentionally; keep these artifacts **off GitHub** unless reviewed for sensitive content. Logs are excerpts, not complete retention of every request. For presentation, copy the complete run directory to Windows:

```powershell
scp -P 22 -r root@89.42.199.195:/opt/distributed-banking-platform/evidence/phase7/ <WINDOWS_DESTINATION>
```

Replace `<WINDOWS_DESTINATION>` with an actual Windows folder path in PowerShell (the example is intentionally a placeholder).

## Failure policy / acceptance

* Exit on first failed step. Never quietly skip a scenario or count local unit tests as VPS proof.
* Stop on PENDING/UNKNOWN 2PC outcomes; inspect the exact UUID, status API, Coordinator log, and both `pg_prepared_xacts` tables. Do not retry a financial request with a different ID.
* If an injected crash cannot be restored, restore **only the affected Compose service**, keeping its volumes.
* Do not run `docker compose down -v`, prune volumes, manually `ROLLBACK PREPARED`, or reset balances.
* Phase 7 is accepted only when **seven distinct IDs are PASS** in a `summary.json` from the VPS and money conservation holds.
