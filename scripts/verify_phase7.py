#!/usr/bin/env python3
"""Seven required Assignment 2 experiments, with auditable evidence.

Run from the repository root on the VPS (only local Gateway at 127.0.0.1:8001).

  python3 scripts/verify_phase7.py --list
  python3 scripts/verify_phase7.py --experiment 7
  python3 scripts/verify_phase7.py --all --allow-disruptive

Experiment 4 stops ONLY bank_a1 (and restores it); experiment 6 crashes
ONLY coordinator three times (and restarts it). No database cleanup/reset.
Any failure halts the suite. Inspect durable transaction IDs before rerun.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
EXPERIMENTS = {
    1: ('Shard distribution + correct Gateway owner routing', [
        ('Persistent placement and ownership', ['scripts/verify_phase2.py']),
        ('Gateway account routing', ['scripts/verify_phase3.py'])]),
    2: ('Replica load distribution + replica log evidence', [
        ('80+80 concurrent reads, replica identity', ['scripts/verify_phase3.py'])]),
    3: ('Concurrent deposits and withdrawals across replicas', [
        ('Concurrent writes and balance invariants', ['scripts/verify_phase4.py'])]),
    4: ('Replica interruption under concurrent read load', [
        ('Stop bank_a1, continue A2, restore A1', ['scripts/verify_phase3.py', '--failover'])]),
    5: ('Local and cross-shard transfers', [
        ('Atomic local transfers', ['scripts/verify_phase4.py']),
        ('Prepared 2PC and permanent decisions', ['scripts/verify_phase5.py'])]),
    6: ('Interrupted Coordinator with durable COMMIT', [
        ('Crash after commit and recovery checks', ['scripts/verify_phase6.py', '--crash-tests'])]),
    7: ('Concurrent overlapping cross-shard transfers', [
        ('Distinct UUIDv4 per overlapping 2PC transfer', ['scripts/verify_phase7_concurrent_2pc.py'])]),
}
DISRUPTIVE = frozenset({4, 6})
LOG_SERVICES = ('gateway', 'coordinator', 'bank_a1', 'bank_a2', 'bank_b1', 'bank_b2')


def stamp():
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def invoke(args, *, timeout=20, cwd=ROOT):
    try:
        process = subprocess.run(args, cwd=cwd, text=True, stdout=subprocess.PIPE,
                                 stderr=subprocess.STDOUT, timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(f'Command timeout {args}: {exc}') from exc
    if process.returncode:
        raise RuntimeError(f'Command failed (rc={process.returncode}) {args}: {process.stdout[-2000:]}')
    return process.stdout


def preflight():
    invoke(['docker', 'compose', 'config', '--quiet'])
    status = invoke(['docker', 'compose', 'ps', '--all'])
    # All active components must have been brought up by the operator first.
    for service in ('bank_a1', 'bank_a2', 'bank_b1', 'bank_b2', 'coordinator', 'gateway', 'shard_a_db', 'shard_b_db'):
        if 'distributed-banking-' + service.replace('_', '_') not in status:
            raise RuntimeError(f'Missing active service {service}; see docker compose ps --all')
    # A one-shot init job Exited (0) is normal and not counted unhealthy.
    for shard in ('a', 'b'):
        value = invoke(['docker', 'compose', 'exec', '-T', f'shard_{shard}_db',
                        'psql', '-U', 'bank', '-d', f'bank_{shard}', '-Atc',
                        'SELECT count(*) FROM pg_prepared_xacts WHERE database=current_database()'])
        if value.strip() != '0':
            raise RuntimeError(f'Shard {shard} has {value.strip()} unresolved prepared transactions; STOP')
    return status


def print_and_record(step_file: Path, command: list[str]) -> tuple[int, float]:
    started = time.monotonic()
    with step_file.open('w', encoding='utf-8') as stream:
        banner = '$ ' + ' '.join(command) + '\n'
        print(banner, end='', flush=True)
        stream.write(banner)
        stream.flush()
        proc = subprocess.Popen(command, cwd=ROOT, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, text=True, bufsize=1)
        assert proc.stdout is not None
        for line in proc.stdout:
            print(line, end='', flush=True)
            stream.write(line)
            stream.flush()
        rc = proc.wait()
        elapsed = round(time.monotonic() - started, 2)
        stream.write(f'\nEXIT_CODE={rc} ELAPSED_SECONDS={elapsed}\n')
        return rc, elapsed


def save(path: Path, obj: dict):
    tmp = path.with_suffix('.tmp')
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True) + '\n', encoding='utf-8')
    os.replace(tmp, path)


def capture_evidence(path: Path):
    # Collect only service statuses and application logs. No Docker inspect
    # environment, .env, passwords, or arbitrary volume/database dumps.
    try:
        (path / 'docker_compose_ps.txt').write_text(
            invoke(['docker', 'compose', 'ps', '--all'], timeout=25), encoding='utf-8')
    except Exception as exc:
        (path / 'docker_compose_ps.txt').write_text('UNAVAILABLE: ' + str(exc), encoding='utf-8')
    try:
        (path / 'docker_application_logs.txt').write_text(
            invoke(['docker', 'compose', 'logs', '--no-color', '--tail=140', *LOG_SERVICES], timeout=35),
            encoding='utf-8')
    except Exception as exc:
        (path / 'docker_application_logs.txt').write_text('UNAVAILABLE: ' + str(exc), encoding='utf-8')
    try:
        (path / 'assignment1_unchanged_snapshot.txt').write_text(
            invoke(['docker', 'ps', '--filter', 'name=mini-banking', '--format', '{{.Names}} {{.Status}} {{.Ports}}']),
            encoding='utf-8')
    except Exception as exc:
        (path / 'assignment1_unchanged_snapshot.txt').write_text('UNAVAILABLE: ' + str(exc), encoding='utf-8')


def check_selected(numbers, allow_disruptive):
    bad = sorted(set(numbers) - set(EXPERIMENTS))
    if bad:
        raise ValueError(f'Unknown experiment IDs: {bad}')
    if any(x in DISRUPTIVE for x in numbers) and not allow_disruptive:
        raise ValueError('Experiments 4 and 6 deliberately stop containers. Repeat with --allow-disruptive.')


def write_markdown(path: Path, summary: dict) -> None:
    rows = [
        '# Assignment 2 — Phase 7 evidence',
        '',
        f"- **Status:** {summary.get('status', 'UNKNOWN')}",
        f"- **Started (UTC):** {summary.get('started_utc', '')}",
        f"- **Finished (UTC):** {summary.get('finished_utc', '')}",
        '- **Source:** Original assignment §6 (seven required experiments)',
        '',
        '| No. | Experiment | Result |',
        '| --- | --- | --- |',
    ]
    found = {r['number']: r for r in summary.get('results', [])}
    for number, (name, _) in EXPERIMENTS.items():
        row = found.get(number, {})
        result = row.get('status', 'NOT_RUN')
        rows.append(f'| {number} | {name} | {result} |')
    if 'failure' in summary:
        rows += ['', '## Failure / STOP', '', f"`{summary['failure']}`"]
    rows += ['', 'Individual step logs, PostgreSQL checks, replica logs, and transaction',
             'UUIDs are stored alongside `summary.json`. A missing test is **NOT_RUN**, not PASS.', '']
    path.write_text('\n'.join(rows), encoding='utf-8')


def run(numbers, output_dir, *, allow_disruptive=False, count=16, workers=3):
    check_selected(numbers, allow_disruptive)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=False)
    summary = {
        'assignment': 'Assignment 2: Distributed Banking Platform',
        'source': 'Original assignment section 6 - 7 required experiments',
        'started_utc': stamp(),
        'experiment_ids': list(numbers),
        'disruptive_enabled': allow_disruptive,
        'harness_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        'status': 'IN_PROGRESS',
        'results': [],
    }
    summary_path = output_dir / 'summary.json'
    save(summary_path, summary)
    (output_dir / 'initial').mkdir()
    capture_evidence(output_dir / 'initial')
    try:
        summary['preflight'] = preflight()
        save(summary_path, summary)
        for exp_id in numbers:
            name, steps = EXPERIMENTS[exp_id]
            folder = output_dir / f'experiment_{exp_id:02d}'
            folder.mkdir()
            experiment = {'number': exp_id, 'name': name, 'started_utc': stamp(),
                          'status': 'RUNNING', 'steps': []}
            summary['results'].append(experiment)
            save(summary_path, summary)
            print(f'\n========== EXPERIMENT {exp_id}/7: {name} ==========', flush=True)
            for step_n, (step_name, script_args) in enumerate(steps, 1):
                command = [sys.executable, '-u', *script_args]
                if exp_id == 7:
                    command.extend(['--count', str(count), '--workers', str(workers),
                                    '--evidence', str((folder / 'transaction_evidence.json').resolve())])
                filename = f'step_{step_n:02d}.txt'
                record = {'name': step_name, 'command': command[1:], 'output_file': filename,
                          'status': 'RUNNING'}
                experiment['steps'].append(record)
                save(summary_path, summary)
                rc, elapsed = print_and_record(folder / filename, command)
                record.update(return_code=rc, duration_seconds=elapsed,
                              status='PASS' if rc == 0 else 'FAIL')
                save(summary_path, summary)
                if rc:
                    raise RuntimeError(f'Experiment {exp_id} step {step_n} failed. STOP. See {folder / filename}')
            # Database consistency must be checked only after all steps resolve.
            for shard in ('a', 'b'):
                count_prepared = invoke(['docker', 'compose', 'exec', '-T', f'shard_{shard}_db',
                    'psql', '-U', 'bank', '-d', f'bank_{shard}', '-Atc',
                    'SELECT count(*) FROM pg_prepared_xacts WHERE database=current_database()'])
                if count_prepared.strip() != '0':
                    raise RuntimeError(f'Experiment {exp_id}: pending PostgreSQL PREPARED on shard {shard}: {count_prepared}')
            capture_evidence(folder)
            experiment['status'] = 'PASS'
            experiment['finished_utc'] = stamp()
            save(summary_path, summary)
        summary['status'] = 'PASS'
        return summary
    except BaseException as exc:
        if summary['results'] and summary['results'][-1]['status'] == 'RUNNING':
            summary['results'][-1]['status'] = 'FAIL'
            summary['results'][-1]['finished_utc'] = stamp()
        summary['status'] = 'STOPPED' if isinstance(exc, Exception) else 'INTERRUPTED'
        summary['failure'] = str(exc)
        print(f'\nSTOP: {exc}', file=sys.stderr, flush=True)
        raise
    finally:
        summary['finished_utc'] = stamp()
        save(summary_path, summary)
        write_markdown(output_dir / 'SUMMARY.md', summary)
        final = output_dir / 'final'
        final.mkdir(exist_ok=True)
        capture_evidence(final)
        print(f'\nEvidence stored at: {output_dir}', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--list', action='store_true', help='Show the 7 experiments, no commands executed')
    group.add_argument('--experiment', type=int, choices=range(1, 8), help='Run exactly one experiment')
    group.add_argument('--all', action='store_true', help='Run all 7 experiments in PDF order')
    parser.add_argument('--allow-disruptive', action='store_true',
                        help='Explicitly allow stopping only bank_a1 and coordinator')
    parser.add_argument('--evidence-root', default='evidence/phase7',
                        help='Evidence folder (a unique run folder is created beneath it)')
    parser.add_argument('--count', type=int, default=16,
                        help='Number of experiment 7 2PC operations, multiple of 8')
    parser.add_argument('--workers', type=int, default=3, help='Parallel 2PC workers, 2..6')
    args = parser.parse_args()
    if args.list or (not args.all and args.experiment is None):
        for exp_id, (name, _steps) in EXPERIMENTS.items():
            flag = ' (DISRUPTIVE)' if exp_id in DISRUPTIVE else ''
            print(f'{exp_id}. {name}{flag}')
        print('\nUse --experiment N or --all --allow-disruptive')
        return
    numbers = list(EXPERIMENTS) if args.all else [args.experiment]
    try:
        check_selected(numbers, args.allow_disruptive)
        run_name = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '-' + uuid4().hex[:8]
        folder = Path(args.evidence_root).resolve() / run_name
        run(numbers, folder, allow_disruptive=args.allow_disruptive,
            count=args.count, workers=args.workers)
    except (RuntimeError, ValueError, OSError) as exc:
        print(f'FAIL/STOP: {exc}', file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
