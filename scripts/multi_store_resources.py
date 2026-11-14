#!/usr/bin/env python3
"""Measure many simultaneously open isolated database fixtures on Linux."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import statistics
import subprocess
import tempfile
import time

from benchmarks.telemetry import difference, quiet_window, read, snapshot
from campaign_report import ReportWriter

MODES = ('ordinary', 'checkpoint', 'sqlite')


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def invoke(binary, cpu, *args):
    completed = subprocess.run(['taskset', '-c', str(cpu), binary, *map(str, args)],
                               check=True, text=True, capture_output=True, timeout=120)
    return json.loads(completed.stdout)


def validate(sample, mode, projects, keys):
    if (sample.get('status') != 'verified' or sample.get('mode') != mode
            or sample.get('projects') != projects or sample.get('keys_per_project') != keys
            or sample.get('verified_reads') != projects * 3):
        raise ValueError('measurement failed its fixture/read checks')
    fields = ('rss_before_kib', 'rss_open_kib', 'rss_after_reads_kib', 'rss_after_drop_kib',
              'fds_before', 'fds_open', 'fds_after_drop', 'open_total_ns', 'lookup_total_ns',
              'read_bytes', 'rchar')
    if any(type(sample.get(name)) is not int or sample[name] < 0 for name in fields):
        raise ValueError('invalid resource counter')
    times = sample.get('open_samples_ns')
    if (not isinstance(times, list) or len(times) != projects
            or any(type(value) is not int or value < 0 for value in times)
            or sample['fds_open'] < sample['fds_before']):
        raise ValueError('invalid project-open samples')
    return sample


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--binary', default='target/release/examples/multi_store_resources')
    parser.add_argument('--projects', default='1,10,100')
    parser.add_argument('--keys', type=int, default=1000)
    parser.add_argument('--rounds', type=int, default=3)
    args = parser.parse_args()
    try:
        counts = [int(item) for item in args.projects.split(',')]
    except ValueError:
        parser.error('project counts must be comma-separated integers')
    if (not counts or len(set(counts)) != len(counts) or any(not 1 <= n <= 128 for n in counts)
            or not 1 <= args.keys <= 10_000 or max(counts) * args.keys > 200_000
            or not 2 <= args.rounds <= 20 or not Path(args.directory).is_dir()
            or not Path(args.binary).is_file() or os.path.lexists(args.output)
            or os.path.lexists(args.output + '.progress.json')):
        parser.error('existing directory/binary, new output and bounded fixture dimensions required')
    allowed = sorted(os.sched_getaffinity(0))
    if len(allowed) < 2:
        parser.error('at least two allowed CPUs are required')
    worker, driver = allowed[0], allowed[-1]
    os.sched_setaffinity(0, {driver})
    binary = str(Path(args.binary).resolve())
    output = Path(args.output)
    result = {'revision': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
              'git_status': subprocess.check_output(['git', 'status', '--short'], text=True),
              'binary_sha256': digest(binary), 'driver_sha256': digest(__file__),
              'report_writer_sha256': digest('scripts/campaign_report.py'),
              'example_sha256': digest('examples/multi_store_resources.rs'),
              'measurement_sha256': digest('examples/multi_store_resources/linux.rs'),
              'cargo_lock_sha256': digest('Cargo.lock'), 'allowed_cpus': allowed,
              'worker_cpu': worker, 'driver_cpu': driver, 'cpu_quota': read('/sys/fs/cgroup/cpu.max'),
              'host_platform': platform.platform(),
              'cpu_model': next((line.split(':', 1)[1].strip() for line in Path('/proc/cpuinfo').read_text().splitlines()
                                 if line.startswith('model name')), 'unknown'),
              'filesystem': json.loads(subprocess.check_output(
                  ['findmnt', '-J', '-T', args.directory, '-o', 'TARGET,FSTYPE'], text=True)),
              'project_counts': counts, 'keys_per_project': args.keys, 'rounds': args.rounds,
              'started_utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
              'scope': ('Warm-cache per-process samples with one open store per project, sampled reads, '
                        'and SQLite WAL/FULL. Process RSS excludes page cache and is not a quota; '
                        'open/recovery semantics differ. No network service, isolation, scale-to-zero, '
                        'cold device or universal performance ranking is established.'),
              'completed': False, 'failure': None, 'cases': []}
    with output.open('x', encoding='utf-8') as file:
        json.dump(result, file, indent=2)
        file.write('\n')
    writer = ReportWriter(output, 0)
    with writer.progress.open('x', encoding='utf-8') as file:
        json.dump({'revision': result['revision'], 'completed': False, 'failure': None}, file)
        file.write('\n')

    def save():
        writer.save(result, force=True)

    try:
        with tempfile.TemporaryDirectory(prefix='multi-store-resource-', dir=args.directory) as fixture:
            prepared = invoke(binary, worker, 'prepare', fixture, max(counts), args.keys)
            if (prepared.get('status') != 'prepared' or prepared.get('projects') != max(counts)
                    or prepared.get('keys_per_project') != args.keys
                    or prepared.get('v2_stores', 0) + prepared.get('v3_stores', 0) != max(counts)
                    or prepared.get('sqlite_journal') != 'WAL' or prepared.get('sqlite_synchronous') != 'FULL'):
                raise ValueError('fixture preparation did not match its settings')
            result['fixture'] = prepared
            save()
            for count in counts:
                for iteration in range(args.rounds):
                    modes = MODES[iteration % 3:] + MODES[:iteration % 3]
                    if (iteration // 3) % 2:
                        modes = tuple(reversed(modes))
                    for mode in modes:
                        quiet = quiet_window(seconds=0.25, attempts=3)
                        before = snapshot()
                        sample = validate(invoke(binary, worker, 'measure', fixture, count, args.keys, mode),
                                          mode, count, args.keys)
                        noise = difference(before, snapshot())
                        result['cases'].append({'projects': count, 'round': iteration + 1, 'mode': mode,
                                                'quiet': quiet, 'telemetry': noise, 'sample': sample})
                        save()
                        print(f'{count} projects, round {iteration + 1}, {mode}: '
                              f'RSS after reads {sample["rss_after_reads_kib"]} KiB', flush=True)
            result['summary'] = []
            for count in counts:
                for mode in MODES:
                    samples = [case['sample'] for case in result['cases']
                               if case['projects'] == count and case['mode'] == mode]
                    if len(samples) != args.rounds:
                        raise ValueError('missing measurement sample')
                    result['summary'].append({'projects': count, 'mode': mode,
                        'median_rss_after_reads_kib': statistics.median(s['rss_after_reads_kib'] for s in samples),
                        'median_rss_increase_kib': statistics.median(
                            s['rss_after_reads_kib'] - s['rss_before_kib'] for s in samples),
                        'median_fds_open': statistics.median(s['fds_open'] for s in samples),
                        'median_open_total_ns': statistics.median(s['open_total_ns'] for s in samples)})
            result['completed'] = True
    except Exception as error:
        result['failure'] = f'{type(error).__name__}: {error}'
        raise
    finally:
        result['finished_utc'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
        save()


if __name__ == '__main__':
    main()
