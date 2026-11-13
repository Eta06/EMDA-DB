#!/usr/bin/env python3
"""Test an interrupted completed sync and either a successful or failed retry."""
import argparse
import errno
import hashlib
import itertools
import json
import os
from pathlib import Path
import platform
import subprocess
import tempfile
import time

from campaign_report import ReportWriter
import sync_eio_campaign as base


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def check_trace(path, syscall, mode):
    lines = []
    for line in path.read_text().splitlines():
        call, number, actual, reported, error = line.split()
        lines.append({'call': call, 'number': int(number), 'actual': int(actual),
                      'reported': int(reported), 'errno': int(error)})
    expected_first = {'call': syscall, 'number': 1, 'actual': 0, 'reported': -1, 'errno': errno.EINTR}
    expected_second = {'call': syscall, 'number': 2, 'actual': -2 if mode == 'error' else 0,
                       'reported': -1 if mode == 'error' else 0,
                       'errno': errno.EIO if mode == 'error' else 0}
    if lines != [expected_first, expected_second]:
        raise ValueError(f'missing completed sync, interruption or expected retry: {lines}')
    return lines


def run_case(binary, library, directory, fmt, target, syscall, operation, mode, order):
    with tempfile.TemporaryDirectory(prefix='sync-interrupt-', dir=directory) as temporary:
        root = Path(temporary)
        if base.run_probe(binary, 'setup', root, fmt) != {'status': 'ready', 'format': fmt}:
            raise ValueError('invalid setup result')
        before = base.snapshot(root, fmt)
        trace = root / 'sync-interrupt.trace'
        env = os.environ.copy()
        env.update(LD_PRELOAD=str(library), EMDA_SYNC_TARGET=str(root / 'source' / target),
                   EMDA_SYNC_MODE=mode, EMDA_SYNC_TRACE=str(trace))
        run = subprocess.run([str(binary), 'attempt', str(root), operation], env=env,
                             capture_output=True, text=True, timeout=60)
        if run.returncode:
            raise RuntimeError(f'attempt failed: {run.stderr[-3000:]}')
        outcome = json.loads(run.stdout)
        if outcome.get('status') != mode or outcome.get('operation') != operation:
            raise ValueError(f'operation incorrectly reported its outcome: {outcome}')
        events = check_trace(trace, syscall, mode)
        after = base.snapshot(root, fmt)
        base.check_source(before, after, fmt, operation)
        verification = base.run_probe(binary, 'verify', root, operation, mode, order)
        backends = ['ordinary', 'checkpoint'] if order == 'ordinary-first' else ['checkpoint', 'ordinary']
        if (verification.get('status') != 'verified' or verification.get('recovery_order') != backends
                or verification.get('new_present') is not True):
            raise ValueError('recovery did not verify the complete attempted write')
        final = base.snapshot(root, fmt)
        base.check_source(before, final, fmt, operation)
        return {'format': fmt, 'target': target, 'syscall': syscall, 'operation': operation,
                'mode': mode, 'order': order, 'outcome': outcome, 'events': events,
                'verification': verification, 'before': base.describe(before),
                'after': base.describe(after), 'after_verification': base.describe(final)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', required=True)
    parser.add_argument('--binary', required=True)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    directory, binary, output = (Path(value).resolve() for value in
                                  (args.directory, args.binary, args.output))
    if (not directory.is_dir() or not binary.is_file() or os.path.lexists(output)
            or os.path.lexists(str(output) + '.progress.json')):
        parser.error('existing fixture directory and binary, and new output, required')
    source = Path(__file__).resolve().with_name('sync_interrupt_inject.c')
    report = {'revision': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
              'git_status': subprocess.check_output(['git', 'status', '--short'], text=True),
              'binary_sha256': digest(binary), 'interposer_source_sha256': digest(source),
              'driver_sha256': digest(__file__), 'platform': platform.platform(),
              'filesystem': json.loads(subprocess.check_output(
                  ['findmnt', '-J', '-T', str(directory), '-o', 'TARGET,FSTYPE'], text=True)),
              'started_utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
              'completed': False, 'failure': None, 'current': None, 'cases': []}
    with output.open('x', encoding='utf-8') as file:
        json.dump(report, file, indent=2)
        file.write('\n')
    writer = ReportWriter(output, 1.0)
    with writer.progress.open('x', encoding='utf-8') as file:
        json.dump({name: report.get(name) for name in ('revision', 'current', 'completed', 'failure')}, file)
        file.write('\n')
    try:
        with tempfile.TemporaryDirectory(prefix='sync-interrupt-lib-', dir=directory) as temporary:
            library = Path(temporary) / 'sync-interrupt.so'
            subprocess.run(['cc', '-shared', '-fPIC', '-std=c11', '-Wall', '-Wextra', '-Werror',
                            '-O2', '-o', str(library), str(source)], check=True, timeout=60)
            report['interposer_sha256'] = digest(library)
            for (fmt, target, syscall), operation, mode, order in itertools.product(
                    (('v2', 'records.log', 'fdatasync'), ('v2', 'commit.state', 'fdatasync'),
                     ('v3', 'records.log', 'fsync')),
                    ('write', 'batch', 'checkpoint-write', 'checkpoint-batch'),
                    ('success', 'error'), ('ordinary-first', 'checkpoint-first')):
                report['current'] = {'format': fmt, 'target': target, 'syscall': syscall,
                                     'operation': operation, 'mode': mode, 'order': order}
                writer.save(report)
                report['cases'].append(run_case(binary, library, directory, fmt, target, syscall,
                                                operation, mode, order))
                writer.save(report)
            report['current'] = None
            report['completed'] = True
    except Exception as error:
        report['failure'] = f'{type(error).__name__}: {error}'
        raise
    finally:
        report['finished_utc'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
        writer.save(report, force=True)
    print(f"completed {len(report['cases'])} interrupted-sync cases")


if __name__ == '__main__':
    main()
