#!/usr/bin/env python3
"""Compare verified process-crash restarts after a larger durable fixture."""
import argparse
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import tempfile
import time

from benchmarks.large_crash import BACKENDS, check
from benchmarks.telemetry import parse_cpus, quiet_window, read


def digest(path):
    value = hashlib.sha256()
    with Path(path).open('rb') as file:
        for part in iter(lambda: file.read(1024 * 1024), b''):
            value.update(part)
    return value.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--server', default='target/release/examples/network_bench_server')
    parser.add_argument('--prototype-preparer', default='target/release/examples/index_memory_bench')
    parser.add_argument('--sqlite-preparer', default='target/release/examples/sqlite_restart_write_bench')
    parser.add_argument('--backends', nargs='+', choices=BACKENDS, default=BACKENDS)
    parser.add_argument('--keys', type=int, default=100_000)
    parser.add_argument('--rounds', type=int, default=1)
    parser.add_argument('--client-cpus', default='0')
    parser.add_argument('--server-cpus', default='1-3')
    args = parser.parse_args()
    binaries = [Path(args.server), Path(args.prototype_preparer), Path(args.sqlite_preparer)]
    try:
        client_cpus, server_cpus = parse_cpus(args.client_cpus), parse_cpus(args.server_cpus)
    except ValueError as error:
        parser.error(str(error))
    allowed = os.sched_getaffinity(0)
    if (not Path(args.directory).is_dir() or not Path(args.output).parent.is_dir()
            or os.path.lexists(args.output) or any(not path.is_file() for path in binaries)
            or not 1 <= args.keys <= 1_000_000 or not 1 <= args.rounds <= 10
            or len(set(args.backends)) != len(args.backends) or not client_cpus or not server_cpus
            or client_cpus & server_cpus or not (client_cpus | server_cpus) <= allowed):
        parser.error('invalid paths, fixture size, backends, rounds or CPU affinity')
    args.directory = str(Path(args.directory).resolve())
    args.server, args.prototype_preparer, args.sqlite_preparer = (str(path.resolve()) for path in binaries)
    os.sched_setaffinity(0, client_cpus)
    sources = [Path(__file__), Path('scripts/benchmarks/large_crash.py'),
               Path('scripts/compare_crashes.py'), Path('scripts/benchmarks/services.py'),
               Path('scripts/benchmarks/clients.py'), Path('examples/network_bench_server.rs'),
               Path('examples/index_memory_bench.rs'), Path('examples/sqlite_restart_write_bench.rs'),
               Path('Cargo.lock'), *Path('src').rglob('*.rs')]
    versions = {}
    for package in ('psycopg', 'pymongo'):
        try:
            versions[package] = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            versions[package] = None
    result = {
        'started_utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'revision': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
        'git_status': subprocess.check_output(['git', 'status', '--short'], text=True),
        'source_sha256': {str(path): digest(path) for path in sorted(sources)},
        'binary_sha256': {path.name: digest(path) for path in binaries},
        'python': sys.version, 'platform': platform.platform(), 'dependencies': versions,
        'filesystem': json.loads(subprocess.check_output(
            ['findmnt', '-J', '-T', args.directory, '-o', 'TARGET,FSTYPE'], text=True)),
        'cpu_quota': read('/sys/fs/cgroup/cpu.max'), 'allowed_cpus': sorted(allowed),
        'client_cpus': sorted(client_cpus), 'server_cpus': sorted(server_cpus),
        'keys': args.keys, 'key_bytes': 8, 'value_bytes': 64, 'rounds': args.rounds,
        'backends': list(args.backends), 'completed': False, 'failure': None,
        'current': None, 'runs': [],
        'scope': ('Fresh disposable fixtures; acknowledged seed batches and 120 individual inserts, '
                  'updates and deletes; SIGKILL after responses, restart, then verify every value and deletion. '
                  'The OS and device caches remain running. Restart readiness includes different adapter, '
                  'container and polling overhead and is not an equivalent recovery benchmark. '
                  'Process and container resource samples have different scopes, and fixture sizes use '
                  'different methods. No physical power failure, distributed fault, off-host restore or '
                  'in-flight commit interruption is tested.'),
    }
    output = Path(args.output)
    with output.open('x', encoding='utf-8') as file:
        json.dump(result, file, indent=2)
        file.write('\n')

    def save():
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', dir=output.parent,
                                         prefix='.large-crash-report-', delete=False) as file:
            temporary = Path(file.name)
            try:
                json.dump(result, file, indent=2)
                file.write('\n')
                file.flush()
                os.replace(temporary, output)
            finally:
                temporary.unlink(missing_ok=True)

    try:
        for index in range(args.rounds):
            rotation = index % len(args.backends)
            for backend in args.backends[rotation:] + args.backends[:rotation]:
                result['current'] = {'round': index + 1, 'backend': backend}
                save()
                quiet = quiet_window()
                try:
                    outcome = check(backend, args.directory, args.server, args.prototype_preparer,
                                    args.sqlite_preparer, args.keys, args.server_cpus)
                except Exception as error:
                    outcome = {'backend': backend, 'result': 'failed', 'error': f'{type(error).__name__}: {error}'}
                result['runs'].append({'round': index + 1, 'pre_run_quiet': quiet, **outcome})
                save()
                print(f'round={index + 1} backend={backend} result={outcome["result"]}', flush=True)
        result['current'] = None
        result['completed'] = True
    except Exception as error:
        result['failure'] = f'{type(error).__name__}: {error}'
        raise
    finally:
        result['finished_utc'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
        save()
    if any(run['result'] != 'passed' for run in result['runs']):
        raise SystemExit('one or more crash checks failed; see retained result')


if __name__ == '__main__':
    main()
