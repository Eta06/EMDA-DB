#!/usr/bin/env python3
"""Inject EIO at real synchronization syscalls on disposable Linux fixtures."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import tempfile
import time

SYSCALLS = ('fsync', 'fdatasync')
OPERATIONS = ('write', 'backup', 'restore', 'upgrade')
FORMATS = ('v2', 'v3')
CALL = re.compile(r'\b(fsync|fdatasync)\(')


def digest(data):
    return hashlib.sha256(data).hexdigest()


def snapshot(root, format_name):
    source = root / 'source'
    log = (source / 'records.log').read_bytes()
    witness = (source / 'commit.state').read_bytes()
    metadata = os.getxattr(source / 'records.log', 'user.db.commit.v3') if format_name == 'v3' else None
    backup = (root / 'reference-backup').read_bytes()
    return {'log': log, 'witness': witness, 'metadata': metadata, 'backup': backup}


def describe(state):
    return {name + '_sha256': digest(value) if value is not None else None for name, value in state.items()} | {
        'log_bytes': len(state['log']), 'backup_bytes': len(state['backup'])}


def check_source(before, after, format_name, operation):
    if not after['log'].startswith(before['log']):
        raise ValueError('acknowledged log prefix changed')
    if after['backup'] != before['backup']:
        raise ValueError('reference backup changed')
    if operation != 'write' and after != before:
        raise ValueError('copy operation changed the source or reference backup')
    if format_name == 'v3' and after['witness'] != before['witness']:
        raise ValueError('V3 identity anchor changed')


def run_probe(binary, *args):
    run = subprocess.run([binary, *map(str, args)], capture_output=True, text=True, timeout=60)
    if run.returncode:
        raise RuntimeError(f'probe exited {run.returncode}: {run.stderr[-3000:]}')
    return json.loads(run.stdout)


def traced_attempt(binary, root, operation, syscall=None, ordinal=None):
    trace = root / 'trace.txt'
    command = ['strace', '-f', '-qq', '-yy', '-o', str(trace), '-e', 'trace=fsync,fdatasync']
    if syscall:
        command += ['-e', f'inject={syscall}:error=EIO:when={ordinal}']
    command += [binary, 'attempt', str(root), operation]
    run = subprocess.run(command, capture_output=True, text=True, timeout=60)
    if run.returncode:
        raise RuntimeError(f'traced probe exited {run.returncode}: {run.stderr[-3000:]}')
    outcome = json.loads(run.stdout)
    if outcome.get('operation') != operation or outcome.get('status') not in ('success', 'error'):
        raise ValueError('unexpected action outcome')
    lines = trace.read_text().splitlines()
    counts = {
        name: sum(1 for line in lines if (match := CALL.search(line)) and match.group(1) == name)
        for name in SYSCALLS
    }
    injected = [line for line in lines if '(INJECTED)' in line]
    if syscall and (len(injected) != 1 or not re.search(r'\b' + syscall + r'\(', injected[0])):
        raise ValueError(f'{syscall} injection was not observed exactly once')
    if not syscall and injected:
        raise ValueError('control run unexpectedly reported an injection')
    return {'outcome': outcome, 'counts': counts, 'injected_trace': injected[0] if injected else None}


def run_case(binary, parent, format_name, operation, syscall=None, ordinal=None):
    with tempfile.TemporaryDirectory(prefix='sync-eio-case-', dir=parent) as path:
        root = Path(path)
        setup = run_probe(binary, 'setup', root, format_name)
        if setup != {'status': 'ready', 'format': format_name}:
            raise ValueError('unexpected fixture setup')
        before = snapshot(root, format_name)
        traced = traced_attempt(binary, root, operation, syscall, ordinal)
        after = snapshot(root, format_name)
        check_source(before, after, format_name, operation)
        verification = run_probe(binary, 'verify', root, operation, traced['outcome']['status'])
        if verification.get('status') != 'verified' or verification.get('operation') != operation:
            raise ValueError('recovery verification did not complete')
        final = snapshot(root, format_name)
        check_source(before, final, format_name, operation)
        return {**traced, 'verification': verification, 'before': describe(before),
                'after_attempt': describe(after), 'after_verification': describe(final)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', required=True, help='existing directory for disposable fixtures')
    parser.add_argument('--binary', default='target/release/examples/sync_failure_probe')
    parser.add_argument('--output', required=True, help='new JSON result path')
    parser.add_argument('--max-ordinals', type=int, default=0,
                        help='limit each syscall per case; zero tests all baseline ordinals')
    args = parser.parse_args()
    if (not Path(args.directory).is_dir() or not Path(args.binary).is_file() or os.path.lexists(args.output)
            or not 0 <= args.max_ordinals <= 100):
        parser.error('existing fixture directory and binary, new output, and valid ordinal limit required')
    binary = str(Path(args.binary).resolve())
    result = {
        'revision': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
        'git_status': subprocess.check_output(['git', 'status', '--short'], text=True),
        'binary_sha256': digest(Path(binary).read_bytes()),
        'probe_sha256': digest(Path('examples/sync_failure_probe.rs').read_bytes()),
        'driver_sha256': digest(Path(__file__).read_bytes()),
        'cargo_lock_sha256': digest(Path('Cargo.lock').read_bytes()),
        'strace': subprocess.check_output(['strace', '--version'], text=True).splitlines()[0],
        'host_platform': platform.platform(),
        'max_ordinals': args.max_ordinals,
        'filesystem': json.loads(subprocess.check_output(
            ['findmnt', '-J', '-T', args.directory, '-o', 'TARGET,FSTYPE'], text=True)),
        'started_utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'scope': ('Inject a returned EIO at each selected fsync or fdatasync ordinal from a successful '
                  'single-process path. Verify acknowledged values, source/backup preservation and any '
                  'visible copy on the running OS. Failure-only extra syscalls, other syscall types, '
                  'crash ordering, physical power loss and off-host recovery are not exhausted.'),
        'completed': False,
        'failure': None,
        'current': None,
        'cases': [],
    }
    output = Path(args.output)
    with output.open('x', encoding='utf-8') as file:
        json.dump(result, file, indent=2)
        file.write('\n')

    def save():
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', dir=output.parent,
                                         prefix='.sync-eio-report-', delete=False) as file:
            temporary = Path(file.name)
            try:
                json.dump(result, file, indent=2)
                file.write('\n')
                file.flush()
                os.replace(temporary, output)
            finally:
                temporary.unlink(missing_ok=True)

    try:
        with tempfile.TemporaryDirectory(prefix='sync-eio-campaign-', dir=args.directory) as temp:
            for format_name in FORMATS:
                for operation in OPERATIONS:
                    result['current'] = {'format': format_name, 'operation': operation, 'stage': 'control'}
                    save()
                    control = run_case(binary, temp, format_name, operation)
                    if control['outcome']['status'] != 'success':
                        raise ValueError(f'control action failed for {format_name}/{operation}')
                    case = {'format': format_name, 'operation': operation, 'control': control, 'injections': []}
                    result['cases'].append(case)
                    save()
                    for syscall in SYSCALLS:
                        count = control['counts'][syscall]
                        if args.max_ordinals:
                            count = min(count, args.max_ordinals)
                        for ordinal in range(1, count + 1):
                            result['current'] = {'format': format_name, 'operation': operation,
                                                 'syscall': syscall, 'ordinal': ordinal}
                            save()
                            injection = run_case(binary, temp, format_name, operation, syscall, ordinal)
                            case['injections'].append({'syscall': syscall, 'ordinal': ordinal, **injection})
                            save()
                    print(f'{format_name}/{operation}: {len(case["injections"])} injected failures', flush=True)
        result['current'] = None
        result['completed'] = True
    except Exception as error:
        result['failure'] = f'{type(error).__name__}: {error}'
        raise
    finally:
        result['finished_utc'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
        save()
    print('total injections',sum(len(case['injections']) for case in result['cases']))


if __name__ == '__main__':
    main()
