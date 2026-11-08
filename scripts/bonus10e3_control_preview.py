"""CLI preview: create/read durable cutover journal, NEVER perform DB writes.

This command deliberately does not accept operator-supplied proofs to advance
state; a trusted PostgreSQL observer/runner must be integrated first.
"""
from __future__ import annotations
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bank_platform.migration.control_plane import DurableMigrationJournal, MigrationPlan, new_plan


def main():
    p = argparse.ArgumentParser(description='Read-only control-plane preview; no production cutover')
    p.add_argument('--journal', type=Path, required=True)
    p.add_argument('--init', action='store_true', help='Create an offline plan journal (no database access)')
    p.add_argument('--migration-id', help='UUID v4 for an immutable offline plan')
    args = p.parse_args()
    if args.init:
        if args.journal.exists():
            p.error('Journal already exists; refusing to overwrite it')
        plan = (MigrationPlan(args.migration_id) if args.migration_id else new_plan())
        plan.validate()
        DurableMigrationJournal(args.journal).initialize(plan)
    else:
        if not args.journal.exists():
            p.error('Journal does not exist; use --init')
    journal = DurableMigrationJournal(args.journal)
    print(json.dumps({'status': journal.status(), 'history': journal.history(),
                      'database_changes': False, 'gateway_changes': False}, sort_keys=True))
    print('BONUS10E3_CONTROL_PLANE_PREVIEW_OK_NO_LIVE_MIGRATION')


if __name__ == '__main__':
    main()
