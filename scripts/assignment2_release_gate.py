#!/usr/bin/env python3
"""Static, safe release invariants. No network, database, Docker mutation or secrets."""
from __future__ import annotations
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LAB = ROOT / 'labs/distributed-banking-b10e5-crash-lab'
REQUIRED = [
    ROOT / 'compose.yaml', ROOT / 'compose.release.yaml', ROOT / 'Dockerfile',
    ROOT / 'bank_platform/migration/control_plane.py',
    ROOT / 'bank_platform/migration/epoch.py',
    ROOT / 'bank_platform/migration/postgres_staging.py',
    ROOT / 'bank_platform/migration/shard_cohort.py',
    ROOT / 'scripts/self_healing_controller.py',
    LAB / 'compose.b10e5.yaml', LAB / 'scripts/crash_matrix.sh',
    LAB / 'scripts/verify_final.py',
    LAB / 'database/bonus10e4c2_physical_owner_guard.sql',
    ROOT / 'docs/BONUS10E_FINAL_RELEASE_HANDOFF_FA.md',
]
for path in REQUIRED:
    assert path.is_file(), f'Missing release prerequisite: {path.relative_to(ROOT)}'

for path in (ROOT / 'compose.yaml', ROOT / 'compose.release.yaml', LAB / 'compose.b10e5.yaml'):
    content = path.read_text(encoding='utf-8')
    assert re.search(r'(?m)^\s*restart:\s*["\']no["\']\s*$', content), f'Explicit no-restart policy missing in {path}'

release = (ROOT / 'compose.release.yaml').read_text(encoding='utf-8')
assert re.search(r'docker\.io/alimahdibahrami/distributed-banking-platform@sha256:[0-9a-f]{64}', release), 'Release image digest not pinned'

tracked = subprocess.run(['git', 'ls-files', '-z'], cwd=ROOT, check=True, capture_output=True).stdout.decode().split('\0')
for rel in filter(None, tracked):
    p = Path(rel)
    assert p.name != '.env', f'Secret file tracked: {rel}'
    assert not rel.endswith(('.sqlite3','.sqlite3-wal','.sqlite3-shm')), f'Database state tracked: {rel}'
    assert not rel.startswith('evidence/'), f'Sensitive live evidence tracked: {rel}'

print('ASSIGNMENT2_RELEASE_STATIC_GATE_PASS')
print('Scope: tracked source, core image and isolated lab configuration; not live availability or full bonus grading.')
