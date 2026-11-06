#!/usr/bin/env python3
"""Verify a retried V3 witness still requires successful synchronization."""
import argparse
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import tempfile
import time

import sync_eio_campaign as base


def validate_trace(lines, root, sync_error, fault='write'):
    """Require an interrupted witness access, equivalent retry and log fsync."""
    source = f'<{root / "source" / "records.log"}>'
    syscall = {'write': 'fsetxattr', 'read': 'fgetxattr'}[fault]
    witness = [index for index, line in enumerate(lines)
               if syscall + '(' in line and source in line and '"user.db.commit.v3"' in line]
    interrupted = [index for index in witness if base.reports_injected_error(lines[index], 'EINTR')]
    if len(interrupted) != 1:
        raise ValueError('expected one interrupted V3 witness access')
    first = interrupted[0]
    following = [index for index in witness if index > first]
    if not following:
        raise ValueError('interrupted witness access was not retried')
    retry = following[0]

    def arguments(line):
        if fault == 'read':
            # strace prints an address on failure but the returned buffer on
            # success. Compare the descriptor, attribute and requested size.
            match = re.search(r'\bfgetxattr\((\d+<[^>]+>), ("[^"]+"), .*?, (\d+)\)\s+=', line)
        else:
            match = re.search(r'\bfsetxattr\((.*)\)\s+=', line)
        if not match:
            raise ValueError('could not parse witness access')
        return match.groups()
    success = r'=\s*\d+\s*$' if fault == 'read' else r'=\s*0\s*$'
    if arguments(lines[first]) != arguments(lines[retry]) or not re.search(success, lines[retry]):
        raise ValueError('witness retry changed arguments or failed')
    syncs = [line for line in lines[retry + 1:] if 'fsync(' in line and source in line]
    if not syncs:
        raise ValueError('successful witness retry was not followed by log fsync')
    if sync_error:
        if not base.reports_injected_error(syncs[0], 'EIO'):
            raise ValueError('expected subsequent log fsync to return EIO')
    elif not re.search(r'=\s*0\s*$', syncs[0]):
        raise ValueError('expected subsequent log fsync to succeed')
    return [lines[first], lines[retry], syncs[0]]


def tracer(sync_error, fault):
    def trace(binary, root, operation, syscall, ordinal, persistent, syscalls, fixture_only):
        if syscall is not None or ordinal is not None or persistent or fixture_only:
            raise ValueError('unexpected retry-fault configuration')
        output = root / 'trace.txt'
        syscall = {'write': 'fsetxattr', 'read': 'fgetxattr'}[fault]
        command = ['strace', '-f', '-qq', '-yy', '-s', '512', '-o', str(output),
                   '-P', str(root / 'source' / 'records.log'), '-e', 'trace=fsetxattr,fgetxattr,fsync',
                   '-e', f'inject={syscall}:error=EINTR:when=1']
        if sync_error:
            command += ['-e', 'inject=fsync:error=EIO:when=1']
        command += [binary, 'attempt', str(root), operation]
        run = subprocess.run(command, capture_output=True, text=True, timeout=60)
        if run.returncode:
            raise RuntimeError(f'traced probe exited {run.returncode}: {run.stderr[-3000:]}')
        outcome = json.loads(run.stdout)
        expected = 'error' if sync_error else 'success'
        if outcome.get('operation') != operation or outcome.get('status') != expected:
            raise ValueError(f'expected operation {expected}, got {outcome}')
        trace = validate_trace(output.read_text().splitlines(), root, sync_error, fault)
        return {'outcome': outcome, 'selected_trace': trace}
    return trace


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', required=True)
    parser.add_argument('--binary', default='target/release/examples/sync_failure_probe')
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    if not Path(args.directory).is_dir() or not Path(args.binary).is_file() or os.path.lexists(args.output):
        parser.error('existing fixture parent and binary and new output required')
    directory = str(Path(args.directory).resolve())
    binary = str(Path(args.binary).resolve())
    output = Path(args.output)
    result = {
        'revision': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
        'git_status': subprocess.check_output(['git', 'status', '--short'], text=True),
        'binary_sha256': base.digest(Path(binary).read_bytes()),
        'driver_sha256': base.digest(Path(__file__).read_bytes()),
        'strace': subprocess.check_output(['strace', '--version'], text=True).splitlines()[0],
        'host_platform': platform.platform(),
        'filesystem': json.loads(subprocess.check_output(
            ['findmnt', '-J', '-T', directory, '-o', 'TARGET,FSTYPE'], text=True)),
        'started_utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'completed': False, 'failure': None, 'current': None, 'cases': [],
    }
    with output.open('x', encoding='utf-8') as file:
        json.dump(result, file, indent=2)

    def save():
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', dir=output.parent,
                                         prefix='.witness-retry-', delete=False) as file:
            temporary = Path(file.name)
            try:
                json.dump(result, file, indent=2)
                file.write('\n')
                file.flush()
                os.replace(temporary, output)
            finally:
                temporary.unlink(missing_ok=True)

    try:
        for operation in ('write', 'checkpoint-write', 'batch', 'checkpoint-batch'):
            for fault in ('write', 'read'):
                for sync_error in (False, True):
                    for order in base.RECOVERY_ORDERS:
                        result['current'] = {'operation': operation, 'fault': fault,
                                             'sync_error': sync_error, 'order': order}
                        save()
                        checked = base.run_case(binary, directory, 'v3', operation, order=order,
                                                tracer=tracer(sync_error, fault))
                        result['cases'].append({**result['current'], **checked})
                        save()
        result['current'] = None
        result['completed'] = True
    except Exception as error:
        result['failure'] = f'{type(error).__name__}: {error}'
        raise
    finally:
        result['finished_utc'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
        save()
    print(f'verified {len(result["cases"])} interrupted witness and synchronization cases')


if __name__ == '__main__':
    main()
