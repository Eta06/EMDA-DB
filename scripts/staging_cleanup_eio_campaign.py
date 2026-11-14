#!/usr/bin/env python3
"""Inject returned filesystem errors into disposable detailed staging cleanup."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import stat
import subprocess
import tempfile
import time

from campaign_report import ReportWriter
from sync_eio_campaign import SYSCALL_SETS, run_probe, traced_attempt

FILE_MARKER = '.emda-stage-v1-f-aaaaaaaaaaaaaaaa.owner'
DIR_MARKER = '.emda-stage-v1-d-bbbbbbbbbbbbbbbb.owner'
LINK_MARKER = '.emda-stage-v1-f-cccccccccccccccc.owner'
RETIRED_MARKER = '.emda-stage-v1-r-dddddddddddddddd.owner'
BAD_MARKER = '.emda-stage-v1-f-eeeeeeeeeeeeeeee.owner'
REMOVABLE = (FILE_MARKER, DIR_MARKER)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def prepare(root):
    (root / 'sentinel').write_bytes(b'preserve unrelated source data')
    (root / FILE_MARKER).write_bytes(b'EMDA-STAGING-V1-FILE\n')
    (root / (FILE_MARKER + '.data')).write_bytes(b'abandoned file')
    (root / DIR_MARKER).write_bytes(b'EMDA-STAGING-V1-DIRECTORY\n')
    directory = root / (DIR_MARKER + '.data')
    directory.mkdir()
    (directory / 'nested').write_bytes(b'abandoned directory payload')
    (directory / 'link').symlink_to('../sentinel')
    (root / LINK_MARKER).write_bytes(b'EMDA-STAGING-V1-FILE\n')
    (root / (LINK_MARKER + '.data')).symlink_to('sentinel')
    (root / RETIRED_MARKER).write_bytes(b'EMDA-STAGING-V1-RETIRED\n' + bytes(20))
    (root / (RETIRED_MARKER + '.data')).mkdir()
    (root / (RETIRED_MARKER + '.data') / 'keep').write_bytes(b'preserve retired data')
    (root / BAD_MARKER).write_bytes(b'invalid marker')


def snapshot(root):
    result = {}
    def visit(directory):
        for path in sorted(directory.iterdir()):
            relative = path.relative_to(root).as_posix()
            if relative == 'trace.txt':
                continue
            mode = path.lstat().st_mode
            if stat.S_ISLNK(mode):
                result[relative] = {'kind': 'symlink', 'target': os.readlink(path)}
            elif stat.S_ISDIR(mode):
                result[relative] = {'kind': 'directory'}
                visit(path)
            elif stat.S_ISREG(mode):
                data = path.read_bytes()
                result[relative] = {'kind': 'file', 'sha256': digest(data), 'bytes': len(data)}
            else:
                raise ValueError('unexpected fixture entry')
    visit(root)
    return result


def is_removable(path):
    return any(path == marker or path == marker + '.data' or
               path.startswith(marker + '.data/') for marker in REMOVABLE)


def check_states(before, after, final, outcome, resumed):
    if any(path not in before or value != before[path] for path, value in after.items()):
        raise ValueError('attempt changed or introduced fixture data')
    if any(path not in after for path in before if not is_removable(path)):
        raise ValueError('attempt removed protected fixture data')
    if outcome['status'] == 'error':
        if outcome.get('classification') not in ('not_started', 'unknown'):
            raise ValueError('invalid cleanup classification')
        if outcome['classification'] == 'not_started' and after != before:
            raise ValueError('not-started cleanup changed the fixture')
    elif outcome['status'] != 'success' or outcome.get('classification') is not None:
        raise ValueError('invalid cleanup outcome')
    elif any(is_removable(path) for path in after):
        raise ValueError('successful cleanup left a recognized stage')
    for marker in REMOVABLE:
        if marker not in after and any(path == marker + '.data' or
                                       path.startswith(marker + '.data/') for path in after):
            raise ValueError('removed marker still has a payload')
    if resumed.get('status') != 'success' or any(is_removable(path) for path in final):
        raise ValueError('cleanup could not safely resume')
    if {path: value for path, value in before.items() if not is_removable(path)} != final:
        raise ValueError('cleanup changed protected fixture data')


def run_case(binary, directory, syscalls, error, syscall=None, ordinal=None):
    with tempfile.TemporaryDirectory(prefix='staging-cleanup-case-', dir=directory) as name:
        root = Path(name)
        prepare(root)
        before = snapshot(root)
        traced = traced_attempt(binary, root, 'cleanup', syscall, ordinal, syscalls=syscalls,
                                fixture_only=True, injected_error=error)
        after = snapshot(root)
        resumed = run_probe(binary, 'attempt', root, 'cleanup')
        final = snapshot(root)
        check_states(before, after, final, traced['outcome'], resumed)
        return {**traced, 'fixture': str(root), 'before': before, 'after_attempt': after,
                'after_cleanup': final, 'resumed': resumed}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', required=True)
    parser.add_argument('--binary', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--syscall-set', choices=tuple(SYSCALL_SETS), default='sync')
    parser.add_argument('--error', choices=('EIO', 'EAGAIN', 'EINTR', 'ENOSPC', 'EDQUOT'), default='EIO')
    parser.add_argument('--max-ordinals', type=int, default=0)
    args = parser.parse_args()
    directory = Path(args.directory).resolve()
    binary = Path(args.binary).resolve()
    output = Path(args.output)
    if (not directory.is_dir() or not binary.is_file() or os.path.lexists(output)
            or os.path.lexists(str(output) + '.progress.json') or not 0 <= args.max_ordinals <= 100):
        parser.error('existing directory/binary, new output and valid ordinal limit required')
    if any(character in str(directory) for character in ('"', '\\', '\n', '\r')):
        parser.error('fixture directory contains characters escaped by strace')
    result = {'revision': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
              'git_status': subprocess.check_output(['git', 'status', '--short'], text=True),
              'binary_sha256': digest(binary.read_bytes()), 'driver_sha256': digest(Path(__file__).read_bytes()),
              'probe_sha256': digest(Path('examples/staging_cleanup_failure_probe.rs').read_bytes()),
              'syscall_set': args.syscall_set, 'injected_error': args.error,
              'max_ordinals': args.max_ordinals, 'platform': platform.platform(),
              'strace': subprocess.check_output(['strace', '--version'], text=True).splitlines()[0],
              'started_utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
              'scope': 'One returned error at a selected successful-path call on disposable fixture files; not physical power loss or all failure-only paths.',
              'completed': False, 'failure': None, 'current': None, 'control': None, 'cases': []}
    with output.open('x', encoding='utf-8') as file:
        json.dump(result, file, indent=2)
        file.write('\n')
    writer = ReportWriter(output, 1.0)
    with writer.progress.open('x', encoding='utf-8') as file:
        json.dump({'revision': result['revision'], 'completed': False, 'failure': None}, file)
        file.write('\n')
    try:
        syscalls = SYSCALL_SETS[args.syscall_set]
        control = run_case(str(binary), directory, syscalls, args.error)
        if control['outcome']['status'] != 'success':
            raise ValueError('control cleanup failed')
        result['control'] = control
        writer.save(result, force=True)
        for syscall in syscalls:
            ordinals = control['eligible_ordinals'][syscall]
            if args.max_ordinals:
                ordinals = ordinals[:args.max_ordinals]
            for ordinal in ordinals:
                result['current'] = {'syscall': syscall, 'ordinal': ordinal}
                writer.save(result)
                case = run_case(str(binary), directory, syscalls, args.error, syscall, ordinal)
                result['cases'].append({'syscall': syscall, 'ordinal': ordinal, **case})
                writer.save(result)
        result['current'] = None
        result['completed'] = True
    except Exception as error:
        result['failure'] = f'{type(error).__name__}: {error}'
        raise
    finally:
        result['finished_utc'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
        writer.save(result, force=True)
    print(f"completed {len(result['cases'])} injected cleanup failures")


if __name__ == '__main__':
    main()
