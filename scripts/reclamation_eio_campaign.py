#!/usr/bin/env python3
"""Inject returned filesystem errors into detailed online reclamation."""
import argparse
import json
import os
from pathlib import Path
import platform
import stat
import subprocess
import tempfile
import time

from campaign_report import ReportWriter
import sync_eio_campaign as common

FORMATS = ('v2', 'v3')
ORDERS = ('source-first', 'backup-first')
EXTERNAL = ('checkpoint', 'checkpoint.digest', 'reference-backup', 'original-log.sha256')


def store_state(path):
    if not path.exists():
        return None
    if not path.is_dir() or path.is_symlink():
        raise ValueError('store path is not a regular directory')
    files = {}
    for name in ('records.log', 'commit.state', 'store.lock'):
        item = path / name
        if item.exists():
            if not item.is_file() or item.is_symlink():
                raise ValueError('store file is not regular')
            files[name] = common.digest(item.read_bytes())
        else:
            files[name] = None
    log = path / 'records.log'
    header = log.read_bytes()[:8] if log.exists() else None
    if header == b'EMDALOG3':
        files['format'] = 'v3'
        files['v3_metadata'] = common.digest(os.getxattr(log, 'user.db.commit.v3'))
    elif header == b'EMDALOG2':
        files['format'] = 'v2'
        files['v3_metadata'] = None
    elif header is None:
        files['format'] = None
        files['v3_metadata'] = None
    else:
        raise ValueError('unexpected store log format')
    return files


def stage_state(root):
    entries = {}
    for stage in sorted(root.glob('.emda-stage-v1-*')):
        paths = [stage]
        if stage.is_dir() and not stage.is_symlink():
            paths.extend(sorted(stage.rglob('*')))
        for path in paths:
            info = path.lstat()
            name = str(path.relative_to(root))
            if stat.S_ISREG(info.st_mode):
                content = path.read_bytes()
                entry = {'type': 'file', 'sha256': common.digest(content)}
                if path.name == 'records.log' and content.startswith(b'EMDALOG3'):
                    entry['v3_metadata'] = common.digest(os.getxattr(path, 'user.db.commit.v3'))
                entries[name] = entry
            elif stat.S_ISDIR(info.st_mode):
                entries[name] = {'type': 'directory'}
            else:
                raise ValueError('unexpected stage entry type')
    return entries


def external_state(root):
    return {name: common.digest((root / name).read_bytes()) for name in EXTERNAL}


def check_outcome(outcome, verification, original, source, previous):
    classification = outcome.get('classification')
    if verification['classification'] != (classification or 'success'):
        raise ValueError('verification classification differs')
    if outcome['status'] == 'success':
        if classification is not None or previous is not None or source == original or outcome.get('handle') != 'opened':
            raise ValueError('successful reclamation left an invalid visible state')
    elif outcome['status'] == 'error' and classification == 'not_published':
        if source != original or previous is not None or outcome.get('handle') not in ('opened', 'unopened'):
            raise ValueError('not-published reclamation changed paths')
    elif outcome['status'] == 'error' and classification == 'candidate_unknown':
        if source != original or previous == original:
            raise ValueError('candidate outcome changed original source')
    elif outcome['status'] == 'error' and classification == 'exchange_unknown':
        if previous is None or (source == original) == (previous == original) or outcome.get('handle') != 'blocked':
            raise ValueError('uncertain exchange did not preserve one original or block the handle')
    elif outcome['status'] == 'error' and classification == 'reclamation':
        if source == original or outcome.get('handle') != 'readable':
            raise ValueError('reclamation error did not follow a usable handoff')
    else:
        raise ValueError('invalid reclamation outcome')


def check_cleanup(cleanup, before, after):
    if cleanup.get('status') != 'cleaned' or cleanup.get('backup_restored') is not True:
        raise ValueError('cleanup did not verify live data and backup')
    removed = 0
    for name in ('generic', 'retired'):
        report = cleanup.get(name)
        if not isinstance(report, dict) or set(report) != {'removed_payloads', 'removed_markers', 'skipped'}:
            raise ValueError('invalid cleanup counters')
        if any(type(value) is not int or value < 0 for value in report.values()):
            raise ValueError('invalid cleanup counters')
        removed += report['removed_payloads'] + report['removed_markers']
    if any(name not in before or before[name] != entry for name, entry in after.items()):
        raise ValueError('cleanup added or altered retained staging evidence')
    if (before != after) != (removed > 0):
        raise ValueError('cleanup counters disagree with staged evidence')


def run_case(binary, parent, fmt, syscalls, fixture_only, error_name,
             syscall=None, ordinal=None, order='source-first'):
    with tempfile.TemporaryDirectory(prefix='reclamation-eio-case-', dir=parent) as directory:
        root = Path(directory)
        setup = common.run_probe(binary, 'setup', root, fmt)
        if setup != {'status': 'ready', 'format': fmt}:
            raise ValueError('unexpected reclamation fixture')
        original, external = store_state(root / 'source'), external_state(root)
        traced = common.traced_attempt(binary, root, 'reclaim', syscall, ordinal, False, syscalls,
                                       fixture_only, injected_error=error_name,
                                       command_name='attempt-publication')
        source, previous, stages = store_state(root / 'source'), store_state(root / 'previous'), stage_state(root)
        if external_state(root) != external:
            raise ValueError('reference backup or checkpoint changed')
        classification = traced['outcome'].get('classification') or 'success'
        verified = common.run_probe(binary, 'verify', root, classification, order)
        if verified != {'status': 'verified', 'classification': classification, 'order': order, 'backup_restored': True}:
            raise ValueError('reclamation recovery verification failed')
        check_outcome(traced['outcome'], verified, original, source, previous)
        final_source, final_previous, final_stages, final_external = (store_state(root / 'source'), store_state(root / 'previous'),
                                                                      stage_state(root), external_state(root))
        if (source, previous, stages, external) != (final_source, final_previous, final_stages, final_external):
            raise ValueError('verification changed source, sibling, stage or reference evidence')
        cleanup = common.run_probe(binary, 'cleanup', root)
        cleaned_source, cleaned_previous, cleaned_stages, cleaned_external = (store_state(root / 'source'), store_state(root / 'previous'),
                                                                              stage_state(root), external_state(root))
        if (source, previous, external) != (cleaned_source, cleaned_previous, cleaned_external):
            raise ValueError('cleanup changed source, named sibling or reference evidence')
        check_cleanup(cleanup, stages, cleaned_stages)
        return {**traced, 'original': original, 'source_after': source, 'previous_after': previous,
                'stages_after': stages, 'external': external, 'verification': verified,
                'source_after_verification': final_source, 'previous_after_verification': final_previous,
                'stages_after_verification': final_stages, 'external_after_verification': final_external,
                'cleanup': cleanup, 'source_after_cleanup': cleaned_source, 'previous_after_cleanup': cleaned_previous,
                'stages_after_cleanup': cleaned_stages, 'external_after_cleanup': cleaned_external}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', required=True)
    parser.add_argument('--binary', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--format', choices=FORMATS)
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
              'probe_sha256': common.digest(Path('examples/reclamation_failure_probe.rs').read_bytes()),
              'driver_sha256': common.digest(Path(__file__).read_bytes()),
              'common_driver_sha256': common.digest(Path('scripts/sync_eio_campaign.py').read_bytes()),
              'cargo_lock_sha256': common.digest(Path('Cargo.lock').read_bytes()),
              'strace': subprocess.check_output(['strace', '--version'], text=True).splitlines()[0],
              'host_platform': platform.platform(),
              'filesystem': json.loads(subprocess.check_output(['findmnt', '-J', '-T', directory, '-o', 'TARGET,FSTYPE'], text=True)),
              'max_ordinals': args.max_ordinals, 'syscall_set': args.syscall_set,
              'fixture_paths_only': fixture_only, 'injected_error': args.error,
              'cleanup_checked': True,
              'formats': [args.format] if args.format else list(FORMATS),
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
        with tempfile.TemporaryDirectory(prefix='reclamation-eio-campaign-', dir=directory) as temp:
            for fmt in result['formats']:
                result['current'] = {'format': fmt, 'stage': 'control'}
                writer.save(result)
                control = run_case(binary, temp, fmt, syscalls, fixture_only, args.error)
                if control['outcome']['status'] != 'success':
                    raise ValueError('reclamation control did not succeed')
                case = {'format': fmt, 'control': control, 'injections': []}
                result['cases'].append(case)
                for syscall in syscalls:
                    ordinals = control['eligible_ordinals'][syscall]
                    if args.max_ordinals:
                        ordinals = ordinals[:args.max_ordinals]
                    for ordinal in ordinals:
                        for order in ORDERS:
                            result['current'] = {'format': fmt, 'syscall': syscall, 'ordinal': ordinal, 'order': order}
                            writer.save(result)
                            injection = run_case(binary, temp, fmt, syscalls, fixture_only, args.error, syscall, ordinal, order)
                            case['injections'].append({'syscall': syscall, 'ordinal': ordinal, 'order': order, **injection})
                            writer.save(result)
                print(f'{fmt}: {len(case["injections"])} injected failures', flush=True)
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
