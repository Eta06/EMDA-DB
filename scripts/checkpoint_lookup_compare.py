#!/usr/bin/env python3
"""Paired warm-cache checkpoint lookup comparison with full block checks."""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import statistics
import subprocess
import tempfile
import time

from benchmarks.telemetry import difference, quiet_window, read, snapshot
from index_memory_compare import fields, identity, validate_sample


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def revision(source):
    return subprocess.check_output(['git', '-C', str(source), 'rev-parse', 'HEAD'], text=True).strip()


def is_clean(source):
    return not subprocess.check_output(['git', '-C', str(source), 'status', '--porcelain'], text=True).strip()


def percentile(values, percentage):
    values = sorted(values)
    return values[math.ceil(len(values) * percentage / 100) - 1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', required=True)
    parser.add_argument('--baseline-source', required=True)
    parser.add_argument('--candidate-source', required=True)
    parser.add_argument('--baseline-binary', required=True)
    parser.add_argument('--candidate-binary', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--rounds', type=int, default=5)
    args = parser.parse_args()
    base, candidate = Path(args.baseline_source), Path(args.candidate_source)
    cpus = sorted(os.sched_getaffinity(0))
    if (not Path(args.directory).is_dir() or not base.is_dir() or not candidate.is_dir()
            or not Path(args.baseline_binary).is_file() or not Path(args.candidate_binary).is_file()
            or os.path.lexists(args.output) or not 2 <= args.rounds <= 30 or len(cpus) < 2
            or not is_clean(base) or not is_clean(candidate)):
        parser.error('invalid paths, dirty sources, rounds or CPU availability')
    for relative in ('Cargo.lock', 'examples/index_memory_bench.rs', 'scripts/index_memory_compare.py'):
        if sha(base / relative) != sha(candidate / relative):
            parser.error(f'benchmark input differs: {relative}')
    binaries = {'baseline': args.baseline_binary, 'candidate': args.candidate_binary}
    binary_hashes = {name: sha(path) for name, path in binaries.items()}
    if binary_hashes['baseline'] == binary_hashes['candidate']:
        parser.error('benchmark binaries are identical')
    benchmark_cpu, driver_cpu = cpus[0], cpus[-1]
    os.sched_setaffinity(0, {driver_cpu})
    output = Path(args.output)
    report = {
        'baseline_revision': revision(base), 'candidate_revision': revision(candidate),
        'binary_sha256': binary_hashes, 'script_sha256': sha(__file__),
        'benchmark_source_sha256': sha(base / 'examples/index_memory_bench.rs'),
        'cargo_lock_sha256': sha(base / 'Cargo.lock'),
        'rustc_version': subprocess.check_output(['rustc', '+stable', '--version'], text=True).strip(),
        'platform': platform.platform(), 'cpu_quota': read('/sys/fs/cgroup/cpu.max'),
        'cpu_model': next((line.split(':', 1)[1].strip() for line in Path('/proc/cpuinfo').read_text().splitlines() if line.startswith('model name')), 'unknown'),
        'benchmark_cpu': benchmark_cpu, 'driver_cpu': driver_cpu,
        'filesystem': json.loads(subprocess.check_output(['findmnt', '-J', '-T', args.directory, '-o', 'TARGET,FSTYPE'], text=True)),
        'scope': 'One Linux VM; warm-cache, separate processes and identical prebuilt witnessed fixtures. Each process reopens and verifies 16 fixed base keys and up to 16 suffix keys. The control mode replays the full log without using checkpoint lookups. ABBA pass order; per-variant pooled latency samples repeat the same keys, not independent random requests. Process RSS excludes page cache. No concurrency, physical power loss, off-host restore, cold cache or competitor ranking is measured.',
        'rounds_per_pass': args.rounds, 'cases': [], 'failures': [], 'completed': False,
        'started_utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
    }
    with output.open('x', encoding='utf-8') as file:
        json.dump(report, file, indent=2)
        file.write('\n')

    def save():
        output.write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')

    with tempfile.TemporaryDirectory(prefix='emda-checkpoint-lookup-', dir=args.directory) as root:
        cases = {}
        for count, tail_count in ((16, 0), (1000, 16), (10000, 1024), (200000, 1024)):
            parent = Path(root) / str(count)
            parent.mkdir()
            prepare = subprocess.run(['taskset', '-c', str(benchmark_cpu), binaries['baseline'], 'prepare', str(parent), str(count), '128', str(tail_count)], text=True, capture_output=True, check=True, timeout=300)
            info = fields(prepare.stdout)
            if (int(info['keys']) != count or int(info['key_bytes']) != 128
                    or int(info['tail_keys']) != tail_count or len(info['token']) != 64):
                raise ValueError('unexpected prepared fixture')
            case = {'keys': count, 'key_bytes': 128, 'tail_keys': tail_count, 'log_bytes': int(info['log_bytes']),
                    'checkpoint_bytes': int(info['checkpoint_bytes']), 'fixture_identity': identity(parent), 'runs': []}
            cases[count] = (parent, info['token'], case)
            report['cases'].append(case)
            save()
        for pass_index, variant in enumerate(('baseline', 'candidate', 'candidate', 'baseline')):
            for round_index in range(args.rounds):
                order = [(count, mode) for count in (16, 1000, 10000, 200000)
                         for mode in ('store-full', 'checkpoint-store')]
                shift = (pass_index + round_index) % len(order)
                for count, mode in order[shift:] + order[:shift]:
                    parent, token, case = cases[count]
                    run = {'pass': pass_index + 1, 'round': round_index + 1, 'variant': variant, 'mode': mode,
                           'pre_run_quiet': quiet_window(seconds=.2, attempts=3)}
                    try:
                        before = snapshot()
                        try:
                            command = ['taskset', '-c', str(benchmark_cpu), binaries[variant], 'measure', str(parent), str(count), '128', mode, token, str(case['tail_keys'])]
                            process = subprocess.run(command, capture_output=True, text=True, check=True, timeout=120)
                            run['measurement'] = validate_sample(process.stdout, mode, case['tail_keys'])
                            run['command'] = command
                        finally:
                            run['noise'] = difference(before, snapshot())
                    except Exception as error:
                        run['error'] = f'{type(error).__name__}: {error}'
                        report['failures'].append({'keys': count, 'variant': variant, 'mode': mode, 'pass': pass_index + 1, 'round': round_index + 1, 'error': run['error']})
                    finally:
                        case['runs'].append(run)
                        save()
        for count, (parent, _, case) in cases.items():
            if identity(parent) != case['fixture_identity']:
                report['failures'].append({'keys': count, 'error': 'fixture identity changed'})
        if report['failures']:
            save()
            raise SystemExit('one or more measurements failed; see partial report')
        for case in report['cases']:
            case['summary'] = {}
            for mode in ('store-full', 'checkpoint-store'):
                case['summary'][mode] = {}
                for variant in binaries:
                    samples = [run['measurement'] for run in case['runs'] if run['variant'] == variant and run['mode'] == mode]
                    base_lookups = [value for sample in samples for value in sample['lookups_ns']]
                    tail_lookups = [value for sample in samples for value in sample['tail_lookups_ns']]
                    case['summary'][mode][variant] = {
                        'processes': len(samples), 'lookup_samples': len(base_lookups),
                        'lookup_p50_ns': statistics.median(base_lookups), 'lookup_p95_ns': percentile(base_lookups, 95),
                        'tail_p50_ns': statistics.median(tail_lookups) if tail_lookups else None,
                        'median_open_us': statistics.median(sample['open_us'] for sample in samples),
                        'median_peak_rss_kib': statistics.median(sample['peak_rss_kib'] for sample in samples),
                        'median_read_chars': statistics.median(sample['read_chars'] for sample in samples),
                    }
        report['completed'] = True
        report['finished_utc'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
        save()
    print('Completed', sum(len(case['runs']) for case in report['cases']), 'verified process measurements')


if __name__ == '__main__':
    main()
