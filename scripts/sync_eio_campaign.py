#!/usr/bin/env python3
"""Inject EIO at selected filesystem syscalls on disposable Linux fixtures."""
import argparse
import errno
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import stat
import subprocess
import tempfile
import time

SYSCALL_SETS = {
    'sync': ('fsync', 'fdatasync'),
    'data': ('write', 'writev', 'pwrite64', 'setxattr', 'fsetxattr'),
    'read': ('read', 'readv', 'pread64'),
    'open': ('open', 'openat', 'openat2', 'creat'),
    'namespace': ('mkdir', 'mkdirat', 'rename', 'renameat', 'renameat2',
                  'unlink', 'unlinkat', 'rmdir', 'link', 'linkat'),
}
SYSCALLS = SYSCALL_SETS['sync']
OPERATIONS = ('write', 'backup', 'restore', 'upgrade', 'checkpoint', 'compaction',
              'checkpoint-write', 'checkpoint-backup', 'checkpoint-compaction', 'checkpoint-rollover',
              'initialize')
WRITES = ('write', 'checkpoint-write')
FORMATS = ('v2', 'v3')
RECOVERY_ORDERS = ('ordinary-first', 'checkpoint-first')
CALL = re.compile(r'\b(' + '|'.join(sorted(
    {name for names in SYSCALL_SETS.values() for name in names}, key=len, reverse=True)) + r')\(')
TRACEE = re.compile(r'^\s*(\d+)\s')
FD_CALLS = frozenset(('read', 'readv', 'pread64', 'write', 'writev', 'pwrite64', 'fsetxattr'))


def digest(data):
    return hashlib.sha256(data).hexdigest()


def snapshot(root, format_name):
    source = root / 'source'
    log = (source / 'records.log').read_bytes()
    witness = (source / 'commit.state').read_bytes()
    metadata = os.getxattr(source / 'records.log', 'user.db.commit.v3') if format_name == 'v3' else None
    backup = (root / 'reference-backup').read_bytes()
    checkpoint = (root / 'checkpoint').read_bytes()
    checkpoint_digest = (root / 'checkpoint.digest').read_bytes()
    return {'log': log, 'witness': witness, 'metadata': metadata, 'backup': backup,
            'checkpoint': checkpoint, 'checkpoint_digest': checkpoint_digest}


def describe(state):
    return {name + '_sha256': digest(value) if value is not None else None for name, value in state.items()} | {
        'log_bytes': len(state['log']), 'backup_bytes': len(state['backup'])}


def initialization_snapshot(root):
    target = root / 'result'
    if not target.exists():
        return None
    if not target.is_dir():
        raise ValueError('initialization target is not a directory')
    files = {}
    for name in ('store.lock', 'records.log', 'commit.state'):
        path = target / name
        if path.exists():
            files[name] = path.read_bytes()
    metadata = None
    if 'records.log' in files:
        try:
            metadata = os.getxattr(target / 'records.log', 'user.db.commit.v3')
        except OSError as error:
            if error.errno not in (errno.ENODATA, errno.ENOTSUP, errno.EOPNOTSUPP):
                raise
    return {'files': files, 'metadata': metadata}


def check_initialization(before, after):
    if before is None:
        if after is not None:
            raise ValueError('absent initialization target unexpectedly appeared during verification')
        return
    if after is None:
        raise ValueError('initialization target disappeared during verification')
    for name, value in before['files'].items():
        if after['files'].get(name) != value:
            raise ValueError(f'initialization evidence changed: {name}')
    if 'records.log' in before['files'] and after['metadata'] != before['metadata']:
        raise ValueError('initialization metadata evidence changed')


def describe_initialization(snapshot_value):
    if snapshot_value is None:
        return None
    return {'files': {name: digest(value) for name, value in snapshot_value['files'].items()},
            'metadata_sha256': digest(snapshot_value['metadata']) if snapshot_value['metadata'] is not None else None}


def stage_inventory(root):
    """Check that every visible staging payload still has its valid owner."""
    entries = sorted(path.name for path in root.iterdir() if path.name.startswith('.emda-stage-v1-'))
    for name in entries:
        if not name.endswith('.owner.data'):
            continue
        marker = root / name.removesuffix('.data')
        if not stat.S_ISREG(marker.lstat().st_mode):
            raise ValueError('staging payload owner is not a regular file')
        if name.startswith('.emda-stage-v1-f-'):
            expected = b'EMDA-STAGING-V1-FILE\n'
        elif name.startswith('.emda-stage-v1-d-'):
            expected = b'EMDA-STAGING-V1-DIRECTORY\n'
        else:
            raise ValueError('unexpected staging payload kind')
        if marker.read_bytes() != expected:
            raise ValueError('staging payload lacks a valid ownership marker')
    return entries


def check_source(before, after, format_name, operation):
    if not after['log'].startswith(before['log']):
        raise ValueError('acknowledged log prefix changed')
    if after['backup'] != before['backup']:
        raise ValueError('reference backup changed')
    if (after['checkpoint'] != before['checkpoint']
            or after['checkpoint_digest'] != before['checkpoint_digest']):
        raise ValueError('trusted checkpoint or digest changed')
    if operation not in WRITES and after != before:
        raise ValueError('copy operation changed the source or reference backup')
    if format_name == 'v3' and after['witness'] != before['witness']:
        raise ValueError('V3 identity anchor changed')


def run_probe(binary, *args):
    run = subprocess.run([binary, *map(str, args)], capture_output=True, text=True, timeout=60)
    if run.returncode:
        raise RuntimeError(f'probe exited {run.returncode}: {run.stderr[-3000:]}')
    return json.loads(run.stdout)


def targets_fixture(line, syscall, fixture):
    """Match a syscall argument, never an incidental path in written bytes."""
    path = re.escape(str(fixture))
    if syscall in FD_CALLS:
        return bool(re.search(r'\b' + syscall + r'\(\d+<' + path + r'(?:/|>)', line))
    return bool(re.search(r'[<"]' + path + r'(?:/|[>"])', line))


def traced_ordinals(lines, syscalls, fixture=None):
    """Return per-syscall global ordinals, optionally restricted to the fixture.

    strace's injection counters are per tracee, so path-filtered campaigns
    reject multi-process traces instead of silently targeting a wrong call.
    Counts include unrelated calls, even though they are never selected.
    """
    counts = {name: 0 for name in syscalls}
    eligible = {name: [] for name in syscalls}
    tracees = set()
    for line in lines:
        match = CALL.search(line)
        if not match or match.group(1) not in counts:
            continue
        pid = TRACEE.match(line)
        if pid:
            tracees.add(pid.group(1))
        name = match.group(1)
        counts[name] += 1
        if fixture is None or targets_fixture(line, name, fixture):
            eligible[name].append(counts[name])
    if fixture is not None and len(tracees) > 1:
        raise ValueError('path-filtered campaign cannot index multiple tracees')
    return counts, eligible


def traced_attempt(binary, root, operation, syscall=None, ordinal=None, persistent=False,
                   syscalls=SYSCALLS, fixture_only=False):
    trace = root / 'trace.txt'
    command = ['strace', '-f', '-qq', '-yy', '-s', '512', '-o', str(trace),
               '-e', 'trace=' + ','.join(syscalls)]
    if syscall:
        when = f'{ordinal}+1' if persistent else str(ordinal)
        command += ['-e', f'inject={syscall}:error=EIO:when={when}']
    command += [binary, 'attempt', str(root), operation]
    run = subprocess.run(command, capture_output=True, text=True, timeout=60)
    if run.returncode:
        raise RuntimeError(f'traced probe exited {run.returncode}: {run.stderr[-3000:]}')
    outcome = json.loads(run.stdout)
    if outcome.get('operation') != operation or outcome.get('status') not in ('success', 'error'):
        raise ValueError('unexpected action outcome')
    lines = trace.read_text().splitlines()
    counts, eligible = traced_ordinals(lines, syscalls, root if fixture_only else None)
    injected = [line for line in lines if '(INJECTED)' in line]
    if syscall and (not injected or (not persistent and len(injected) != 1)
                    or any(not re.search(r'\b' + syscall + r'\(', line) for line in injected)):
        raise ValueError(f'{syscall} injection did not match the requested mode')
    if not syscall and injected:
        raise ValueError('control run unexpectedly reported an injection')
    if fixture_only and syscall and any(not targets_fixture(line, syscall, root) for line in injected):
        raise ValueError('injection did not target a fixture filesystem operation')
    return {'outcome': outcome, 'counts': counts, 'eligible_ordinals': eligible,
            'injected_traces': injected}


def run_case(binary, parent, format_name, operation, syscall=None, ordinal=None,
             order='ordinary-first', persistent=False, syscalls=SYSCALLS, fixture_only=False,
             tracer=traced_attempt):
    with tempfile.TemporaryDirectory(prefix='sync-eio-case-', dir=parent) as path:
        root = Path(path)
        setup = run_probe(binary, 'setup', root, format_name)
        if setup != {'status': 'ready', 'format': format_name}:
            raise ValueError('unexpected fixture setup')
        before = snapshot(root, format_name)
        traced = tracer(binary, root, operation, syscall, ordinal, persistent,
                        syscalls, fixture_only)
        after = snapshot(root, format_name)
        check_source(before, after, format_name, operation)
        stages_before = stage_inventory(root)
        initial_target = initialization_snapshot(root) if operation == 'initialize' else None
        verification = run_probe(binary, 'verify', root, operation, traced['outcome']['status'], order)
        expected_order = ['ordinary', 'checkpoint'] if order == 'ordinary-first' else ['checkpoint', 'ordinary']
        if (verification.get('status') != 'verified' or verification.get('operation') != operation
                or verification.get('recovery_order') != expected_order):
            raise ValueError('recovery verification did not complete')
        final = snapshot(root, format_name)
        check_source(before, final, format_name, operation)
        final_target = initialization_snapshot(root) if operation == 'initialize' else None
        if operation == 'initialize':
            check_initialization(initial_target, final_target)
        stages_after = stage_inventory(root)
        if stages_after:
            raise ValueError(f'recognized staging names remain after cleanup: {stages_after}')
        return {**traced, 'verification': verification, 'before': describe(before),
                'after_attempt': describe(after), 'after_verification': describe(final),
                'stages_before_cleanup': stages_before, 'stages_after_cleanup': stages_after,
                'initialization_before_verification': describe_initialization(initial_target),
                'initialization_after_verification': describe_initialization(final_target)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', required=True, help='existing directory for disposable fixtures')
    parser.add_argument('--binary', default='target/release/examples/sync_failure_probe')
    parser.add_argument('--output', required=True, help='new JSON result path')
    parser.add_argument('--max-ordinals', type=int, default=0,
                        help='limit each syscall per case; zero tests all baseline ordinals')
    parser.add_argument('--persistent-errors', action='store_true',
                        help='continue returning EIO for that syscall after the selected ordinal')
    parser.add_argument('--syscall-set', choices=tuple(SYSCALL_SETS), default='sync',
                        help='sync (default), data writes/metadata, reads, opens, or namespace operations')
    args = parser.parse_args()
    if (not Path(args.directory).is_dir() or not Path(args.binary).is_file() or os.path.lexists(args.output)
            or not 0 <= args.max_ordinals <= 100):
        parser.error('existing fixture directory and binary, new output, and valid ordinal limit required')
    if args.persistent_errors and args.syscall_set != 'sync':
        parser.error('persistent errors are supported only for synchronization syscalls')
    binary = str(Path(args.binary).resolve())
    directory = str(Path(args.directory).resolve())
    syscalls = SYSCALL_SETS[args.syscall_set]
    fixture_only = args.syscall_set != 'sync'
    if fixture_only and any(character in directory for character in ('"', '\\', '\n', '\r')):
        parser.error('fixture directory contains characters escaped by strace')
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
        'syscall_set': args.syscall_set,
        'fixture_paths_only': fixture_only,
        'injection_mode': 'persistent-from-ordinal' if args.persistent_errors else 'one-shot',
        'filesystem': json.loads(subprocess.check_output(
            ['findmnt', '-J', '-T', args.directory, '-o', 'TARGET,FSTYPE'], text=True)),
        'started_utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'scope': ('Inject a returned EIO at selected syscall ordinals from a successful '
                  'single-process path. Non-sync sets select fixture filesystem calls only; '
                  'sync includes resolved ancestor directories. In persistent mode, later calls '
                  'of that syscall also fail. '
                  'Test each injection with both writable recovery paths taking '
                  'turns reopening first. Verify acknowledged values, source/backup preservation and any '
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
        with tempfile.TemporaryDirectory(prefix='sync-eio-campaign-', dir=directory) as temp:
            for format_name in FORMATS:
                for operation in OPERATIONS:
                    result['current'] = {'format': format_name, 'operation': operation, 'stage': 'control'}
                    save()
                    control = run_case(binary, temp, format_name, operation,
                                       syscalls=syscalls, fixture_only=fixture_only)
                    if control['outcome']['status'] != 'success':
                        raise ValueError(f'control action failed for {format_name}/{operation}')
                    case = {'format': format_name, 'operation': operation, 'control': control, 'injections': []}
                    result['cases'].append(case)
                    save()
                    for syscall in syscalls:
                        ordinals = control['eligible_ordinals'][syscall]
                        if args.max_ordinals:
                            ordinals = ordinals[:args.max_ordinals]
                        for ordinal in ordinals:
                            for order in RECOVERY_ORDERS:
                                result['current'] = {'format': format_name, 'operation': operation,
                                                     'syscall': syscall, 'ordinal': ordinal, 'order': order}
                                save()
                                injection = run_case(binary, temp, format_name, operation, syscall, ordinal,
                                                     order, args.persistent_errors, syscalls, fixture_only)
                                case['injections'].append({'syscall': syscall, 'ordinal': ordinal,
                                                           'order': order, **injection})
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
