#!/usr/bin/env python3
"""Inject selected returned filesystem errors into disposable compaction handoffs."""
import argparse
import json
import os
from pathlib import Path
import platform
import subprocess
import tempfile
import time

import sync_eio_campaign as common
from campaign_report import ReportWriter

BACKENDS = ('ordinary', 'checkpoint', 'managed', 'online')
FORMATS = ('v2', 'v3')
ORDERS = ('source-first', 'target-first')


def read_optional(path):
    return path.read_bytes() if path.exists() else None


def store_state(path):
    log = (path / 'records.log').read_bytes()
    metadata = os.getxattr(path / 'records.log', 'user.db.commit.v3') if log.startswith(b'EMDALOG3') else None
    return {'log': log, 'witness': (path / 'commit.state').read_bytes(), 'metadata': metadata,
            'selection': read_optional(path / 'checkpoint.selection'),
            'automatic_owner': read_optional(path / '.emda-auto-checkpoints-v1' / 'owner')}


def external_state(root):
    return {name: read_optional(root / name) for name in
            ('reference-backup', 'checkpoint', 'checkpoint.digest', 'original-log.sha256', 'managed-base')}


def describe(state):
    return {name + '_sha256': common.digest(value) if value is not None else None
            for name, value in state.items()}


def check_outcome(outcome, verified):
    classification = outcome.get('classification')
    target = verified.get('target_visible')
    source_old = verified.get('source_original')
    target_old = verified.get('target_original')
    if any(type(value) is not bool for value in (target, source_old, target_old)):
        raise ValueError('missing handoff visibility')
    if source_old == target_old or (not target and not source_old):
        raise ValueError('original log is missing or duplicated')
    if outcome['status'] == 'success':
        if classification is not None or not target or source_old or not target_old:
            raise ValueError('successful handoff did not exchange directories')
    elif classification == 'not_published':
        if target or not source_old:
            raise ValueError('handoff changed paths before publication began')
    elif classification == 'candidate_unknown':
        if not source_old or target_old:
            raise ValueError('exchange occurred before its attempt began')
    elif classification == 'exchange_unknown':
        if not target:
            raise ValueError('exchange attempt lacked a published candidate')
        if verified['operation'] == 'online' and outcome.get('handle_after_exchange_failure') != 'blocked':
            raise ValueError('exchange failure did not block online handle access')
    else:
        raise ValueError('invalid handoff failure classification')


def run_case(binary, parent, fmt, backend, syscall=None, ordinal=None,
             order='source-first', *, syscalls, fixture_only, error_name):
    with tempfile.TemporaryDirectory(prefix='handoff-eio-case-', dir=parent) as directory:
        root = Path(directory)
        setup = common.run_probe(binary, 'setup', root, fmt, backend)
        if setup != {'status': 'ready', 'format': fmt, 'backend': backend}:
            raise ValueError('unexpected handoff fixture setup')
        original = store_state(root / 'source')
        external = external_state(root)
        traced = common.traced_attempt(binary, root, backend, syscall, ordinal, False, syscalls,
                                       fixture_only, injected_error=error_name,
                                       command_name='attempt-publication')
        source_after = store_state(root / 'source')
        target_path = root / 'result'
        target_after = store_state(target_path) if target_path.exists() else None
        external_after = external_state(root)
        if external_after != external:
            raise ValueError('reference backup or trusted checkpoint changed')
        stage_paths = {'root': root, 'source': root / 'source', 'target': target_path}
        stages_before = {name: common.stage_inventory(directory) if directory.exists() else []
                         for name, directory in stage_paths.items()}
        verified = common.run_probe(binary, 'verify', root, backend, order)
        if verified.get('status') != 'verified' or verified.get('operation') != backend or verified.get('order') != order:
            raise ValueError('handoff recovery verification failed')
        if (verified['source_original'] != (source_after == original)
                or verified['target_original'] != (target_after == original)
                or verified['target_visible'] != (target_after is not None)):
            raise ValueError('original log or metadata evidence disagrees with probe')
        check_outcome(traced['outcome'], verified)
        source_final = store_state(root / 'source')
        target_final = store_state(target_path) if target_path.exists() else None
        external_final = external_state(root)
        if source_final != source_after or target_final != target_after or external_final != external:
            raise ValueError('handoff verification changed retained data')
        stages_final = {name: common.stage_inventory(directory) if directory.exists() else []
                        for name, directory in stage_paths.items()}
        if any(stages_final.values()):
            raise ValueError('recognized top-level staging names remain after cleanup')
        return {**traced, 'verification': verified, 'original': describe(original),
                'source_after': describe(source_after),
                'target_after': describe(target_after) if target_after is not None else None,
                'source_after_verification': describe(source_final),
                'target_after_verification': describe(target_final) if target_final is not None else None,
                'external_before': describe(external), 'external_after': describe(external_after),
                'external_after_verification': describe(external_final),
                'stages_before_cleanup': stages_before, 'stages_after_cleanup': stages_final}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', required=True)
    parser.add_argument('--binary', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--format', choices=FORMATS)
    parser.add_argument('--backend', choices=BACKENDS)
    parser.add_argument('--syscall-set', choices=tuple(common.SYSCALL_SETS), default='sync')
    parser.add_argument('--error', choices=('EIO', 'EAGAIN'), default='EIO')
    parser.add_argument('--max-ordinals', type=int, default=0)
    args = parser.parse_args()
    if (not Path(args.directory).is_dir() or not Path(args.binary).is_file()
            or os.path.lexists(args.output) or os.path.lexists(args.output + '.progress.json')
            or not 0 <= args.max_ordinals <= 100):
        parser.error('existing directory and binary, a new output, and valid ordinal limit are required')
    binary = str(Path(args.binary).resolve())
    directory = str(Path(args.directory).resolve())
    syscalls = common.SYSCALL_SETS[args.syscall_set]
    fixture_only = args.syscall_set != 'sync'
    if fixture_only and any(character in directory for character in ('"', '\\', '\n', '\r')):
        parser.error('fixture directory contains characters escaped by strace')
    result = {'revision': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
              'git_status': subprocess.check_output(['git', 'status', '--short'], text=True),
              'binary_sha256': common.digest(Path(binary).read_bytes()),
              'probe_sha256': common.digest(Path('examples/handoff_failure_probe.rs').read_bytes()),
              'driver_sha256': common.digest(Path(__file__).read_bytes()),
              'common_driver_sha256': common.digest(Path('scripts/sync_eio_campaign.py').read_bytes()),
              'cargo_lock_sha256': common.digest(Path('Cargo.lock').read_bytes()),
              'strace': subprocess.check_output(['strace', '--version'], text=True).splitlines()[0],
              'host_platform': platform.platform(), 'max_ordinals': args.max_ordinals,
              'syscall_set': args.syscall_set, 'fixture_paths_only': fixture_only, 'injected_error': args.error,
              'formats': [args.format] if args.format else list(FORMATS),
              'backends': [args.backend] if args.backend else list(BACKENDS),
              'filesystem': json.loads(subprocess.check_output(['findmnt', '-J', '-T', directory, '-o', 'TARGET,FSTYPE'], text=True)),
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
        with tempfile.TemporaryDirectory(prefix='handoff-eio-campaign-', dir=directory) as temp:
            for fmt in result['formats']:
                for backend in result['backends']:
                    result['current'] = {'format': fmt, 'backend': backend, 'stage': 'control'}
                    writer.save(result)
                    control = run_case(binary, temp, fmt, backend, syscalls=syscalls, fixture_only=fixture_only, error_name=args.error)
                    if control['outcome']['status'] != 'success':
                        raise ValueError('handoff control did not succeed')
                    case = {'format': fmt, 'backend': backend, 'control': control, 'injections': []}
                    result['cases'].append(case)
                    for syscall in syscalls:
                        ordinals = control['eligible_ordinals'][syscall]
                        if args.max_ordinals:
                            ordinals = ordinals[:args.max_ordinals]
                        for ordinal in ordinals:
                            for order in ORDERS:
                                result['current'] = {'format': fmt, 'backend': backend, 'syscall': syscall,
                                                     'ordinal': ordinal, 'order': order}
                                writer.save(result)
                                injection = run_case(binary, temp, fmt, backend, syscall, ordinal, order,
                                                     syscalls=syscalls, fixture_only=fixture_only, error_name=args.error)
                                case['injections'].append({'syscall': syscall, 'ordinal': ordinal, 'order': order, **injection})
                                writer.save(result)
                    print(f'{fmt}/{backend}: {len(case["injections"])} injected failures', flush=True)
        result['current'] = None
        result['completed'] = True
    except Exception as error:
        result['failure'] = f'{type(error).__name__}: {error}'
        raise
    finally:
        result['finished_utc'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
        writer.save(result, force=True)
    print('total injections', sum(len(case['injections']) for case in result['cases']))


if __name__ == '__main__':
    main()
