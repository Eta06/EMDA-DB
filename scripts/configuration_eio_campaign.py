#!/usr/bin/env python3
"""Inject returned filesystem errors into automatic-checkpoint configuration."""
import argparse
import json
import os
from pathlib import Path
import platform
import subprocess
import tempfile
import time

from campaign_report import ReportWriter
import sync_eio_campaign as common

FORMATS = ('v2', 'v3')
OPERATIONS = ('initial-enable', 'budgeted-enable', 're-enable', 'disable')
ORDERS = ('ordinary-first', 'managed-first')
MARKER_DIRECTORY = 'source/.emda-auto-checkpoints-v1'
MARKERS = {b'EMDA-AUTO-CHECKPOINT-ROOT-V1:ON\n': 'enabled',
           b'EMDA-AUTO-CHECKPOINT-ROOT-V1:OFF\n': 'disabled'}
FILES = ('source/records.log', 'source/commit.state', 'source/checkpoint.selection',
         'selected', 'checkpoint', 'checkpoint.digest', 'reference-backup')


def snapshot(root):
    files = {name: common.digest((root / name).read_bytes()) for name in FILES}
    log = root / 'source/records.log'
    metadata = os.getxattr(log, 'user.db.commit.v3') if log.read_bytes().startswith(b'EMDALOG3') else None
    files['v3_metadata'] = common.digest(metadata) if metadata is not None else None
    return files


def marker(root):
    parent = root / MARKER_DIRECTORY
    if not parent.exists():
        return 'absent'
    if not parent.is_dir():
        raise ValueError('automatic-checkpoint root is not a directory')
    value = (parent / 'owner').read_bytes()
    if value not in MARKERS:
        raise ValueError('invalid automatic-checkpoint marker')
    return MARKERS[value]


def stages(root):
    directories = {'root': root, 'source': root / 'source', 'configuration': root / MARKER_DIRECTORY}
    return {name: common.stage_inventory(path) if path.exists() else [] for name, path in directories.items()}


def check_outcome(operation, outcome, verification, before, after):
    desired = 'disabled' if operation == 'disable' else 'enabled'
    if before not in ('absent', 'disabled', 'enabled') or after not in ('absent', 'disabled', 'enabled'):
        raise ValueError('unrecognized configuration state')
    expected_before = {'initial-enable': 'absent', 'budgeted-enable': 'absent',
                       're-enable': 'disabled', 'disable': 'enabled'}[operation]
    if before != expected_before or verification['enabled'] != (after == 'enabled'):
        raise ValueError('configuration does not match managed recovery')
    classification = outcome.get('classification')
    if outcome['status'] == 'success':
        if classification is not None or after != desired or outcome.get('handle') != 'opened':
            raise ValueError('successful configuration did not publish desired state')
    elif outcome['status'] == 'error' and classification == 'not_published':
        if after != before or outcome.get('handle') not in ('unopened', 'readable', 'blocked'):
            raise ValueError('not-published configuration changed state')
    elif outcome['status'] == 'error' and classification == 'unknown':
        if after not in (before, desired) or outcome.get('handle') != 'blocked':
            raise ValueError('uncertain configuration lacked a blocked handle or valid state')
    else:
        raise ValueError('invalid configuration outcome')


def run_case(binary, parent, fmt, operation, syscalls, fixture_only, error_name,
             syscall=None, ordinal=None, order='ordinary-first'):
    with tempfile.TemporaryDirectory(prefix='configuration-eio-case-', dir=parent) as directory:
        root = Path(directory)
        setup = common.run_probe(binary, 'setup', root, fmt, operation)
        if setup != {'status': 'ready', 'format': fmt, 'operation': operation}:
            raise ValueError('unexpected configuration fixture')
        original = snapshot(root)
        before = marker(root)
        traced = common.traced_attempt(binary, root, operation, syscall, ordinal, False, syscalls,
                                       fixture_only, injected_error=error_name,
                                       command_name='attempt-publication')
        after = marker(root)
        retained = snapshot(root)
        if retained != original:
            raise ValueError('configuration attempt changed data, selection, checkpoint or backup')
        before_cleanup = stages(root)
        verification = common.run_probe(binary, 'verify', root, operation, order)
        if verification.get('status') != 'verified' or verification.get('operation') != operation or verification.get('order') != order or verification.get('backup_restored') is not True:
            raise ValueError('configuration recovery verification failed')
        check_outcome(operation, traced['outcome'], verification, before, after)
        final = snapshot(root)
        if final != original or marker(root) != after:
            raise ValueError('recovery verification changed retained evidence')
        after_cleanup = stages(root)
        if any(after_cleanup.values()):
            raise ValueError('recognized staging names remain after cleanup')
        return {**traced, 'before': original, 'after_attempt': retained, 'after_verification': final,
                'marker_before': before, 'marker_after': after, 'verification': verification,
                'stages_before_cleanup': before_cleanup, 'stages_after_cleanup': after_cleanup}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', required=True)
    parser.add_argument('--binary', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--format', choices=FORMATS)
    parser.add_argument('--operation', choices=OPERATIONS)
    parser.add_argument('--syscall-set', choices=tuple(common.SYSCALL_SETS), default='sync')
    parser.add_argument('--error', choices=('EIO', 'EAGAIN'), default='EIO')
    parser.add_argument('--max-ordinals', type=int, default=0)
    args = parser.parse_args()
    if not Path(args.directory).is_dir() or not Path(args.binary).is_file() or os.path.lexists(args.output) or os.path.lexists(args.output + '.progress.json') or not 0 <= args.max_ordinals <= 100:
        parser.error('existing directory and binary, a new output, and valid ordinal limit are required')
    directory, binary = str(Path(args.directory).resolve()), str(Path(args.binary).resolve())
    syscalls = common.SYSCALL_SETS[args.syscall_set]
    fixture_only = args.syscall_set != 'sync'
    if fixture_only and any(ch in directory for ch in ('"', '\\', '\n', '\r')):
        parser.error('fixture directory contains characters escaped by strace')
    result = {'revision': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
              'git_status': subprocess.check_output(['git', 'status', '--short'], text=True),
              'binary_sha256': common.digest(Path(binary).read_bytes()),
              'probe_sha256': common.digest(Path('examples/configuration_failure_probe.rs').read_bytes()),
              'driver_sha256': common.digest(Path(__file__).read_bytes()),
              'common_driver_sha256': common.digest(Path('scripts/sync_eio_campaign.py').read_bytes()),
              'cargo_lock_sha256': common.digest(Path('Cargo.lock').read_bytes()),
              'strace': subprocess.check_output(['strace', '--version'], text=True).splitlines()[0],
              'host_platform': platform.platform(),
              'filesystem': json.loads(subprocess.check_output(['findmnt', '-J', '-T', directory, '-o', 'TARGET,FSTYPE'], text=True)),
              'max_ordinals': args.max_ordinals, 'syscall_set': args.syscall_set,
              'fixture_paths_only': fixture_only, 'injected_error': args.error,
              'formats': [args.format] if args.format else list(FORMATS),
              'operations': [args.operation] if args.operation else list(OPERATIONS),
              'started_utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
              'completed': False, 'failure': None, 'current': None, 'cases': []}
    output = Path(args.output)
    with output.open('x', encoding='utf-8') as file:
        json.dump(result, file, indent=2)
        file.write('\n')
    writer = ReportWriter(output, 1.0)
    with writer.progress.open('x', encoding='utf-8') as file:
        json.dump({'revision': result['revision'], 'current': None, 'completed': False, 'failure': None}, file)
        file.write('\n')
    try:
        with tempfile.TemporaryDirectory(prefix='configuration-eio-campaign-', dir=directory) as temp:
            for fmt in result['formats']:
                for operation in result['operations']:
                    result['current'] = {'format': fmt, 'operation': operation, 'stage': 'control'}
                    writer.save(result)
                    control = run_case(binary, temp, fmt, operation, syscalls, fixture_only, args.error)
                    if control['outcome']['status'] != 'success':
                        raise ValueError('configuration control did not succeed')
                    case = {'format': fmt, 'operation': operation, 'control': control, 'injections': []}
                    result['cases'].append(case)
                    for syscall in syscalls:
                        ordinals = control['eligible_ordinals'][syscall]
                        if args.max_ordinals:
                            ordinals = ordinals[:args.max_ordinals]
                        for ordinal in ordinals:
                            for order in ORDERS:
                                result['current'] = {'format': fmt, 'operation': operation, 'syscall': syscall,
                                                     'ordinal': ordinal, 'order': order}
                                writer.save(result)
                                injection = run_case(binary, temp, fmt, operation, syscalls, fixture_only, args.error,
                                                     syscall, ordinal, order)
                                case['injections'].append({'syscall': syscall, 'ordinal': ordinal, 'order': order, **injection})
                                writer.save(result)
                    print(f'{fmt}/{operation}: {len(case["injections"])} injected failures', flush=True)
        result['current'], result['completed'] = None, True
    except Exception as error:
        result['failure'] = f'{type(error).__name__}: {error}'
        raise
    finally:
        result['finished_utc'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
        writer.save(result, force=True)
    print('total injections', sum(len(case['injections']) for case in result['cases']))


if __name__ == '__main__':
    main()
