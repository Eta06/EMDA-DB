#!/usr/bin/env python3
"""Exercise real short writes on disposable Linux store fixtures."""
import argparse
import errno
import hashlib
import itertools
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import tempfile
import time

from campaign_report import ReportWriter
import concurrent_eio_campaign as concurrent
import sync_eio_campaign as base


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def trace_events(path, expected_call, cap, mode):
    events = []
    for line in path.read_text().splitlines():
        call, requested, result, error = line.split()
        events.append({'call': call, 'requested': int(requested),
                       'result': int(result), 'errno': int(error)})
    if not events or any(event['call'] != expected_call for event in events):
        raise ValueError('target call was not intercepted')
    if any(event['requested'] <= 0 or event['result'] > event['requested']
           or (event['result'] >= 0 and event['errno'] != 0) for event in events):
        raise ValueError('invalid intercepted write result')
    if not any(event['requested'] > cap and event['result'] == cap for event in events):
        raise ValueError('no real short write was recorded')
    if mode == 'error':
        if (events[-1]['result'] != -1 or events[-1]['errno'] != errno.EIO
                or any(event['result'] < 0 for event in events[:-1])):
            raise ValueError('EIO was not returned after the short write')
    elif any(event['result'] < 0 for event in events):
        raise ValueError('unexpected error in short-write success case')
    return events


def attempt(binary, library, root, target, cap, mode, operation):
    trace = root / 'short-write.trace'
    env = os.environ.copy()
    env.update(LD_PRELOAD=str(library), EMDA_SHORT_TARGET=str(target),
               EMDA_SHORT_LIMIT=str(cap), EMDA_SHORT_MODE=mode,
               EMDA_SHORT_TRACE=str(trace))
    run = subprocess.run([str(binary), 'attempt', str(root), operation], env=env,
                         capture_output=True, text=True, timeout=60)
    if run.returncode:
        raise RuntimeError(f'attempt failed: {run.stderr[-3000:]}')
    outcome = json.loads(run.stdout)
    if outcome.get('operation') != operation or outcome.get('status') != ('success' if mode == 'success' else 'error'):
        raise ValueError(f'unexpected operation outcome: {outcome}')
    expected_call = 'pwrite' if target.name == 'commit.state' else 'write'
    return outcome, trace_events(trace, expected_call, cap, mode)


def run_case(binary, library, directory, fmt, operation, target_name, cap, mode, order):
    with tempfile.TemporaryDirectory(prefix='short-write-', dir=directory) as temporary:
        root = Path(temporary)
        if base.run_probe(binary, 'setup', root, fmt) != {'status': 'ready', 'format': fmt}:
            raise ValueError('invalid setup result')
        before = base.snapshot(root, fmt)
        target = root / 'source' / target_name
        outcome, events = attempt(binary, library, root, target, cap, mode, operation)
        after = base.snapshot(root, fmt)
        base.check_source(before, after, fmt, operation)
        if mode == 'success':
            verification = base.run_probe(binary, 'verify', root, operation, 'success', order)
            recovery = 'reopened'
        elif target_name == 'records.log':
            if after['witness'] != before['witness'] or after['metadata'] != before['metadata']:
                raise ValueError('witness advanced after a partial log append')
            if len(after['log']) != len(before['log']) + events[0]['result']:
                raise ValueError('partial append length does not match the real short write')
            verification = base.run_probe(binary, 'verify-rejected', root, order)
            recovery = 'rejected'
        elif after['witness'] == before['witness']:
            verification = base.run_probe(binary, 'verify', root, operation, 'error', order)
            if verification.get('new_present') is not True:
                raise ValueError('complete uncertain record was not recovered')
            recovery = 'reopened-uncertain'
        else:
            if after['witness'][cap:] != before['witness'][cap:]:
                raise ValueError('witness bytes beyond the short write changed')
            verification = base.run_probe(binary, 'verify-rejected', root, order)
            recovery = 'rejected'
        final = base.snapshot(root, fmt)
        base.check_source(before, final, fmt, operation)
        if recovery == 'rejected' and final != after:
            raise ValueError('rejected recovery changed file or metadata evidence')
        expected_order = ['ordinary', 'checkpoint'] if order == 'ordinary-first' else ['checkpoint', 'ordinary']
        if verification.get('recovery_order') != expected_order:
            raise ValueError('incorrect recovery order')
        return {'format': fmt, 'operation': operation, 'target': target_name, 'cap': cap,
                'mode': mode, 'order': order, 'outcome': outcome, 'events': events,
                'recovery': recovery, 'verification': verification,
                'before': base.describe(before), 'after': base.describe(after),
                'after_verification': base.describe(final)}


def run_group_case(binary, fixture_binary, library, directory, fmt, cap, mode, order):
    with tempfile.TemporaryDirectory(prefix='short-write-group-', dir=directory) as temporary:
        root = Path(temporary)
        if base.run_probe(fixture_binary, 'setup', root, fmt) != {'status': 'ready', 'format': fmt}:
            raise ValueError('invalid setup result')
        before = base.snapshot(root, fmt)
        trace = root / 'short-write.trace'
        env = os.environ.copy()
        env.update(LD_PRELOAD=str(library), EMDA_SHORT_TARGET=str(root / 'source/records.log'),
                   EMDA_SHORT_LIMIT=str(cap), EMDA_SHORT_MODE=mode,
                   EMDA_SHORT_TRACE=str(trace), EMDA_SHORT_ONLY='writev',
                   EMDA_SHORT_DELAY_FIRST_MS='200')
        run = subprocess.run([str(binary), 'attempt', str(root), 'ordinary', 'single'], env=env,
                             capture_output=True, text=True, timeout=60)
        if run.returncode:
            raise RuntimeError(f'concurrent attempt failed: {run.stderr[-3000:]}')
        receipts = json.loads(run.stdout)
        concurrent.validate_receipts(receipts, require_failed=mode == 'error')
        events = trace_events(trace, 'writev', cap, mode)
        after = base.snapshot(root, fmt)
        base.check_source(before, after, fmt, 'write')
        backends = ['ordinary', 'checkpoint'] if order == 'ordinary-first' else ['checkpoint', 'ordinary']
        if mode == 'success':
            if any(worker['failed'] is not None for worker in receipts['workers']):
                raise ValueError('a short-write success case lost a writer')
            receipt_path = root / 'receipts.json'
            receipt_path.write_text(json.dumps(receipts))
            verification = [concurrent.bounded([str(binary), 'verify', str(root), name,
                                                 str(receipt_path), 'single']) for name in backends]
            concurrent.check_recovery(verification, backends, 'single')
            recovery = 'reopened'
        else:
            verification = base.run_probe(fixture_binary, 'verify-rejected', root, order)
            if verification.get('recovery_order') != backends:
                raise ValueError('incorrect recovery order')
            # We know this test's exact interrupted byte count. Preserve the
            # original evidence and verify acknowledgements on a separate copy
            # after removing only those injected bytes. This is not automatic
            # repair and cannot establish a safe boundary in an unknown crash.
            repaired = root / 'known-tail-copy'
            repaired.mkdir()
            shutil.copytree(root / 'source', repaired / 'source', copy_function=shutil.copy2)
            shutil.copy2(root / 'checkpoint', repaired / 'checkpoint')
            shutil.copy2(root / 'checkpoint.digest', repaired / 'checkpoint.digest')
            copy_log = repaired / 'source/records.log'
            if fmt == 'v3' and os.getxattr(copy_log, 'user.db.commit.v3') != after['metadata']:
                raise ValueError('copy did not preserve witness metadata')
            short = events[0]['result']
            if short != cap or len(after['log']) < len(before['log']) + short:
                raise ValueError('invalid partial group append length')
            os.truncate(copy_log, len(after['log']) - short)
            receipt_path = repaired / 'receipts.json'
            receipt_path.write_text(json.dumps(receipts))
            copy_verification = [concurrent.bounded([str(binary), 'verify', str(repaired), name,
                                                      str(receipt_path), 'single']) for name in backends]
            concurrent.check_recovery(copy_verification, backends, 'single')
            verification['known_tail_copy'] = copy_verification
            recovery = 'rejected'
        final = base.snapshot(root, fmt)
        base.check_source(before, final, fmt, 'write')
        if recovery == 'rejected' and final != after:
            raise ValueError('rejected recovery changed file or metadata evidence')
        return {'format': fmt, 'operation': 'concurrent-group', 'target': 'records.log',
                'cap': cap, 'mode': mode, 'order': order, 'events': events,
                'receipts': receipts, 'recovery': recovery, 'verification': verification,
                'before': base.describe(before), 'after': base.describe(after),
                'after_verification': base.describe(final)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', required=True, help='existing directory for disposable fixtures')
    parser.add_argument('--binary', required=True, help='built sync_failure_probe')
    parser.add_argument('--concurrent-binary', required=True, help='built concurrent_eio_probe')
    parser.add_argument('--output', required=True, help='new result file')
    args = parser.parse_args()
    directory = Path(args.directory).resolve()
    binary = Path(args.binary).resolve()
    concurrent_binary = Path(args.concurrent_binary).resolve()
    output = Path(args.output).resolve()
    if (not directory.is_dir() or not binary.is_file() or not concurrent_binary.is_file()
            or os.path.lexists(output) or os.path.lexists(str(output) + '.progress.json')):
        parser.error('existing fixture directory and binary, and a new output file, are required')
    source = Path(__file__).resolve().with_name('short_write_inject.c')
    report = {'revision': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
              'git_status': subprocess.check_output(['git', 'status', '--short'], text=True),
              'binary_sha256': sha256(binary), 'source_sha256': sha256(source),
              'concurrent_binary_sha256': sha256(concurrent_binary),
              'driver_sha256': sha256(__file__), 'platform': platform.platform(),
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
        with tempfile.TemporaryDirectory(prefix='short-write-lib-', dir=directory) as temporary:
            library = Path(temporary) / 'short-write.so'
            subprocess.run(['cc', '-shared', '-fPIC', '-std=c11', '-Wall', '-Wextra', '-Werror',
                            '-O2', '-o', str(library), str(source)], check=True, timeout=60)
            report['interposer_sha256'] = sha256(library)
            for fmt, operation, mode, order in itertools.product(
                    ('v2', 'v3'), ('write', 'batch', 'checkpoint-write', 'checkpoint-batch'),
                    ('success', 'error'), ('ordinary-first', 'checkpoint-first')):
                targets = [('records.log', (1, 8, 25, 32))]
                if fmt == 'v2':
                    targets.append(('commit.state', (1, 8, 24, 25, 32)))
                for target, caps in targets:
                    for cap in caps:
                        report['current'] = {'format': fmt, 'operation': operation, 'mode': mode,
                                             'order': order, 'target': target, 'cap': cap}
                        writer.save(report)
                        report['cases'].append(run_case(binary, library, directory, fmt, operation,
                                                        target, cap, mode, order))
                        writer.save(report)
            for fmt, mode, order, cap in itertools.product(
                    ('v2', 'v3'), ('success', 'error'),
                    ('ordinary-first', 'checkpoint-first'), (7, 17)):
                report['current'] = {'format': fmt, 'operation': 'concurrent-group',
                                     'mode': mode, 'order': order, 'target': 'records.log', 'cap': cap}
                writer.save(report)
                report['cases'].append(run_group_case(concurrent_binary, binary, library, directory,
                                                      fmt, cap, mode, order))
                writer.save(report)
            report['current'] = None
            report['completed'] = True
    except Exception as error:
        report['failure'] = f'{type(error).__name__}: {error}'
        raise
    finally:
        report['finished_utc'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
        writer.save(report, force=True)
    print(f"completed {len(report['cases'])} short-write cases")


if __name__ == '__main__':
    main()
