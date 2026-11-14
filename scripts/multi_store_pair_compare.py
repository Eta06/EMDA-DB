#!/usr/bin/env python3
"""Compare two multi-store binaries on the same disposable warm fixture."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import statistics
import subprocess
import tempfile
import time

from benchmarks.telemetry import difference, quiet_window, read, snapshot
from campaign_report import ReportWriter
from multi_store_resources import digest, invoke, validate


def source(directory):
    return {'revision': subprocess.check_output(['git', '-C', directory, 'rev-parse', 'HEAD'], text=True).strip(),
            'git_status': subprocess.check_output(['git', '-C', directory, 'status', '--short'], text=True)}


def fixture_identity(root, projects):
    result = hashlib.sha256()
    for index in range(projects):
        project = Path(root) / f'project-{index:04}'
        for name in ('store/records.log', 'store/commit.state', 'checkpoint', 'digest'):
            file = project / name
            result.update(index.to_bytes(4, 'little'))
            result.update(name.encode())
            result.update(bytes.fromhex(digest(file)))
        try:
            metadata = os.getxattr(project / 'store/records.log', 'user.db.commit.v3')
        except OSError as error:
            raise ValueError('paired fixture requires V3 metadata') from error
        result.update(metadata)
    return result.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--baseline-binary', required=True)
    parser.add_argument('--candidate-binary', required=True)
    parser.add_argument('--baseline-source', required=True)
    parser.add_argument('--candidate-source', required=True)
    parser.add_argument('--projects', type=int, default=100)
    parser.add_argument('--keys', type=int, default=1000)
    parser.add_argument('--rounds', type=int, default=3)
    args = parser.parse_args()
    if (not Path(args.directory).is_dir() or not Path(args.baseline_binary).is_file()
            or not Path(args.candidate_binary).is_file() or os.path.lexists(args.output)
            or os.path.lexists(args.output + '.progress.json') or not 1 <= args.projects <= 128
            or not 1 <= args.keys <= 10_000 or args.projects * args.keys > 200_000
            or not 2 <= args.rounds <= 20):
        parser.error('existing directories/binaries, new output and bounded fixture dimensions required')
    allowed = sorted(os.sched_getaffinity(0))
    if len(allowed) < 2:
        parser.error('at least two allowed CPUs are required')
    worker, driver = allowed[0], allowed[-1]
    os.sched_setaffinity(0, {driver})
    binaries = {'baseline': str(Path(args.baseline_binary).resolve()),
                'candidate': str(Path(args.candidate_binary).resolve())}
    sources = {'baseline': source(args.baseline_source), 'candidate': source(args.candidate_source)}
    if any(item['git_status'] for item in sources.values()):
        parser.error('both measured source worktrees must be clean')
    result = {'revision': source('.') , 'sources': sources,
              'binary_sha256': {name: digest(binary) for name, binary in binaries.items()},
              'driver_sha256': digest(__file__), 'measurement_driver_sha256': digest('scripts/multi_store_resources.py'),
              'report_writer_sha256': digest('scripts/campaign_report.py'),
              'allowed_cpus': allowed, 'worker_cpu': worker, 'driver_cpu': driver,
              'cpu_quota': read('/sys/fs/cgroup/cpu.max'),
              'filesystem': json.loads(subprocess.check_output(
                  ['findmnt', '-J', '-T', args.directory, '-o', 'TARGET,FSTYPE'], text=True)),
              'projects': args.projects, 'keys_per_project': args.keys, 'rounds_per_phase': args.rounds,
              'phase_order': ['baseline', 'candidate', 'candidate', 'baseline'],
              'started_utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
              'scope': ('Same V3 fixture and fresh process per sample. Process RSS excludes OS page cache. '
                        'Open times are warm-cache samples on one shared VM, not an isolation, '
                        'durability, cold-device or universal performance claim.'),
              'completed': False, 'failure': None, 'current': None, 'cases': []}
    output = Path(args.output)
    with output.open('x', encoding='utf-8') as file:
        json.dump(result, file, indent=2)
        file.write('\n')
    writer = ReportWriter(output, 0)
    with writer.progress.open('x', encoding='utf-8') as file:
        json.dump({'revision': result['revision'], 'completed': False, 'failure': None}, file)
        file.write('\n')
    try:
        with tempfile.TemporaryDirectory(prefix='multi-store-pair-', dir=args.directory) as fixture:
            prepared = invoke(binaries['baseline'], worker, 'prepare', fixture, args.projects, args.keys)
            if (prepared.get('status') != 'prepared' or prepared.get('projects') != args.projects
                    or prepared.get('keys_per_project') != args.keys
                    or prepared.get('v3_stores') != args.projects):
                raise ValueError('fixture preparation did not match V3 settings')
            result['fixture'] = prepared
            result['fixture_sha256_before'] = fixture_identity(fixture, args.projects)
            writer.save(result, force=True)
            for phase, engine in enumerate(result['phase_order']):
                modes = ('ordinary', 'checkpoint') if phase % 2 == 0 else ('checkpoint', 'ordinary')
                for mode in modes:
                    for iteration in range(args.rounds):
                        result['current'] = {'phase': phase + 1, 'engine': engine, 'mode': mode,
                                             'round': iteration + 1}
                        writer.save(result)
                        quiet = quiet_window(seconds=0.25, attempts=3)
                        before = snapshot()
                        sample = validate(invoke(binaries[engine], worker, 'measure', fixture,
                                                 args.projects, args.keys, mode), mode, args.projects, args.keys)
                        result['cases'].append({**result['current'], 'quiet': quiet,
                                                'telemetry': difference(before, snapshot()), 'sample': sample})
                        writer.save(result)
                        print(f'phase {phase + 1} {engine} {mode} round {iteration + 1}: '
                              f'{sample["fds_open"]} descriptors', flush=True)
            result['fixture_sha256_after'] = fixture_identity(fixture, args.projects)
            if result['fixture_sha256_after'] != result['fixture_sha256_before']:
                raise ValueError('paired database fixture changed during measurements')
            result['summary'] = []
            for phase, engine in enumerate(result['phase_order'], 1):
                for mode in ('ordinary', 'checkpoint'):
                    samples = [case['sample'] for case in result['cases']
                               if case['phase'] == phase and case['engine'] == engine and case['mode'] == mode]
                    if len(samples) != args.rounds:
                        raise ValueError('missing paired sample')
                    result['summary'].append({'phase': phase, 'engine': engine, 'mode': mode,
                        'median_fds_open': statistics.median(s['fds_open'] for s in samples),
                        'median_rss_after_reads_kib': statistics.median(s['rss_after_reads_kib'] for s in samples),
                        'median_open_total_ns': statistics.median(s['open_total_ns'] for s in samples)})
            result['current'] = None
            result['completed'] = True
    except Exception as error:
        result['failure'] = f'{type(error).__name__}: {error}'
        raise
    finally:
        result['finished_utc'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
        writer.save(result, force=True)


if __name__ == '__main__':
    main()
