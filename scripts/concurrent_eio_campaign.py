#!/usr/bin/env python3
"""Inject real witness-write EIO while multiple disposable writers run."""
import argparse
import json
import os
from pathlib import Path
import platform
import re
import signal
import subprocess
import tempfile
import time

import sync_eio_campaign as base

WORKERS = 4
OPERATIONS = 64


def bounded(command):
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               text=True, start_new_session=True)
    try:
        stdout, stderr = process.communicate(timeout=60)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.communicate()
        raise TimeoutError('concurrent EIO probe timed out and its process group was killed')
    if process.returncode:
        raise RuntimeError(f'concurrent EIO probe exited {process.returncode}: {stderr[-3000:]}')
    return json.loads(stdout)


def validate_receipts(report):
    workers = report.get('workers')
    if not isinstance(workers, list) or len(workers) != WORKERS:
        raise ValueError('unexpected writer count')
    for index, worker in enumerate(workers):
        acknowledged = worker.get('acknowledged')
        failed = worker.get('failed')
        if (worker.get('id') != index or not isinstance(acknowledged, list)
                or acknowledged != list(range(len(acknowledged))) or len(acknowledged) > OPERATIONS
                or (failed is not None and (failed != len(acknowledged) or failed >= OPERATIONS))
                or ((failed is None) != (worker.get('error') is None))
                or (failed is None and len(acknowledged) != OPERATIONS)):
            raise ValueError('invalid writer receipt')
    if all(worker['failed'] is None for worker in workers):
        raise ValueError('injected EIO did not fail a writer')


def check_recovery(verified, backends):
    if (len(verified) != 2 or any(item.get('status') != 'verified' or item.get('backend') != name
                                  for item, name in zip(verified, backends))
            or verified[0]['acknowledged'] != verified[1]['acknowledged']
            or verified[0]['uncertain_present'] != verified[1]['uncertain_present']):
        raise ValueError('recovery paths disagree')


def targets_witness(line, root, format_name):
    if format_name == 'v2':
        return ('pwrite64(' in line and f'<{root / "source" / "commit.state"}>' in line)
    if format_name == 'v3':
        return ('fsetxattr(' in line and f'<{root / "source" / "records.log"}>' in line
                and '"user.db.commit.v3"' in line)
    return False


def case(binary, fixture_binary, parent, format_name, backend, ordinal, order):
    with tempfile.TemporaryDirectory(prefix='concurrent-eio-case-', dir=parent) as directory:
        root = Path(directory)
        setup = base.run_probe(fixture_binary, 'setup', root, format_name)
        if setup != {'status': 'ready', 'format': format_name}:
            raise ValueError('unexpected fixture setup')
        before = base.snapshot(root, format_name)
        call = 'pwrite64' if format_name == 'v2' else 'fsetxattr'
        trace = root / 'trace.txt'
        report = bounded(['strace', '-f', '-qq', '-yy', '-s', '128', '-o', str(trace),
                          '-e', 'trace=pwrite64,fsetxattr,writev',
                          '-e', f'inject={call}:error=EIO:when={ordinal}',
                          binary, 'attempt', str(root), backend])
        validate_receipts(report)
        lines = trace.read_text().splitlines()
        injected = [line for line in lines if '(INJECTED)' in line]
        if not injected or any(not targets_witness(line, root, format_name) for line in injected):
            raise ValueError('witness EIO was not observed on the expected source witness')
        after = base.snapshot(root, format_name)
        base.check_source(before, after, format_name, 'write')
        receipt_path = root / 'receipts.json'
        receipt_path.write_text(json.dumps(report))
        backends = ('ordinary', 'checkpoint') if order == 'ordinary-first' else ('checkpoint', 'ordinary')
        verified = [bounded([binary, 'verify', str(root), name, str(receipt_path)]) for name in backends]
        check_recovery(verified, backends)
        final = base.snapshot(root, format_name)
        base.check_source(before, final, format_name, 'write')
        groups = [line for line in lines if re.search(r'\bwritev\(', line)
                  and f'<{root / "source" / "records.log"}>' in line]
        return {'format': format_name, 'backend': backend, 'ordinal_per_tracee': ordinal,
                'recovery_order': order, 'receipts': report, 'verified': verified,
                'injected_traces': injected, 'vectored_append_traces': groups,
                'before': base.describe(before), 'after_attempt': base.describe(after),
                'after_verification': base.describe(final)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', required=True)
    parser.add_argument('--binary', default='target/release/examples/concurrent_eio_probe')
    parser.add_argument('--fixture-binary', default='target/release/examples/sync_failure_probe')
    parser.add_argument('--output', required=True)
    parser.add_argument('--repetitions', type=int, default=3)
    args = parser.parse_args()
    if (not Path(args.directory).is_dir() or not Path(args.binary).is_file()
            or not Path(args.fixture_binary).is_file() or os.path.lexists(args.output)
            or not 1 <= args.repetitions <= 20):
        parser.error('existing fixture parent and binaries, new output and 1..20 repetitions required')
    directory = str(Path(args.directory).resolve())
    if any(character in directory for character in ('"', '\\', '\n', '\r')):
        parser.error('fixture directory contains characters escaped by strace')
    binary = str(Path(args.binary).resolve())
    fixture_binary = str(Path(args.fixture_binary).resolve())
    result = {
        'revision': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
        'git_status': subprocess.check_output(['git', 'status', '--short'], text=True),
        'binary_sha256': base.digest(Path(binary).read_bytes()),
        'fixture_binary_sha256': base.digest(Path(fixture_binary).read_bytes()),
        'probe_sha256': base.digest(Path('examples/concurrent_eio_probe.rs').read_bytes()),
        'driver_sha256': base.digest(Path(__file__).read_bytes()),
        'strace': subprocess.check_output(['strace', '--version'], text=True).splitlines()[0],
        'host_platform': platform.platform(),
        'filesystem': json.loads(subprocess.check_output(
            ['findmnt', '-J', '-T', directory, '-o', 'TARGET,FSTYPE'], text=True)),
        'scope': ('Four independent writers; strace injects a returned witness-write EIO at the '
                  'first or second matching syscall per traced thread. Check all observed '
                  'acknowledgements and each failed write after both recovery orders. '
                  'This does not inject partial writes or model power loss.'),
        'repetitions': args.repetitions, 'started_utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'completed': False, 'failure': None, 'current': None, 'cases': [],
    }
    output = Path(args.output)
    with output.open('x', encoding='utf-8') as file:
        json.dump(result, file, indent=2)
        file.write('\n')

    def save():
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', dir=output.parent,
                                         prefix='.concurrent-eio-', delete=False) as file:
            temporary = Path(file.name)
            try:
                json.dump(result, file, indent=2)
                file.write('\n')
                file.flush()
                os.replace(temporary, output)
            finally:
                temporary.unlink(missing_ok=True)

    try:
        for repetition in range(args.repetitions):
            for format_name in base.FORMATS:
                for backend in ('ordinary', 'checkpoint'):
                    for ordinal in (1, 2):
                        for order in base.RECOVERY_ORDERS:
                            result['current'] = {'repetition': repetition + 1, 'format': format_name,
                                                 'backend': backend, 'ordinal': ordinal, 'order': order}
                            save()
                            measured = case(binary, fixture_binary, directory, format_name, backend, ordinal, order)
                            result['cases'].append({'repetition': repetition + 1, **measured})
                            save()
            print(f'repetition {repetition + 1}: {len(result["cases"])} completed cases', flush=True)
        result['current'] = None
        result['completed'] = True
    except Exception as error:
        result['failure'] = f'{type(error).__name__}: {error}'
        raise
    finally:
        result['finished_utc'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
        save()


if __name__ == '__main__':
    main()
