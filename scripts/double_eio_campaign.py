#!/usr/bin/env python3
"""Inject a filesystem EIO and then an error during later cleanup."""
import argparse
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import tempfile
import time

import sync_eio_campaign as base

SYNC = base.SYSCALL_SETS['sync']
DATA_NAMESPACE = base.SYSCALL_SETS['data'] + base.SYSCALL_SETS['namespace']
PRIMARY = DATA_NAMESPACE + SYNC
CLEANUP = ('unlink', 'unlinkat', 'rmdir')
SECONDARY = SYNC + CLEANUP
TRACED = PRIMARY


def calls_after_primary(lines, primary, root, allow_same_second=False):
    """Find later syncs and fixture removals with per-syscall strace ordinals."""
    counts = dict.fromkeys(SECONDARY, 0)
    selected = {name: [] for name in SECONDARY}
    seen_primary = False
    for line in lines:
        match = base.CALL.search(line)
        if not match:
            continue
        name = match.group(1)
        if name in counts:
            counts[name] += 1
            if seen_primary and (name in SYNC or base.targets_fixture(line, name, root)):
                selected[name].append(counts[name])
        if name == primary and '(INJECTED)' in line:
            if seen_primary and not allow_same_second:
                raise ValueError('primary EIO was injected more than once')
            seen_primary = True
    if not seen_primary:
        raise ValueError('primary EIO was not observed')
    return selected


def injection_rules(syscall, ordinal, second):
    if second is None:
        return ['-e', f'inject={syscall}:error=EIO:when={ordinal}']
    name, number = second
    if name not in SECONDARY:
        raise ValueError('invalid second failure syscall')
    if name == syscall:
        if number <= ordinal:
            raise ValueError('same-syscall second failure must occur later')
        # FIRST..LAST+STEP selects exactly these two ordinals, even if the
        # process invokes this syscall again after the second failure.
        when = f'{ordinal}..{number}+{number - ordinal}'
        return ['-e', f'inject={syscall}:error=EIO:when={when}']
    return ['-e', f'inject={syscall}:error=EIO:when={ordinal}',
            '-e', f'inject={name}:error=EIO:when={number}']


def primary_ordinals(control, syscall, all_sync_paths):
    if all_sync_paths and syscall in SYNC:
        return list(range(1, control['counts'][syscall] + 1))
    return control['eligible_ordinals'][syscall]


def targets_fixture_or_ancestor_sync(line, syscall, root):
    if syscall not in SYNC:
        return False
    match = re.search(r'\b' + re.escape(syscall) + r'\(\d+<([^<>]+)>\)', line)
    if not match:
        return False
    path = Path(match.group(1))
    return path == root or root in path.parents or path in root.parents


def tracer_for(second=None, all_sync_paths=False):
    def trace(binary, root, operation, syscall, ordinal, persistent, syscalls, fixture_only):
        if persistent or not fixture_only or syscall not in PRIMARY or syscalls != TRACED:
            raise ValueError('invalid double-fault configuration')
        trace_path = root / 'trace.txt'
        command = ['strace', '-f', '-qq', '-yy', '-s', '512', '-o', str(trace_path),
                   '-e', 'trace=' + ','.join(TRACED), *injection_rules(syscall, ordinal, second)]
        command += [binary, 'attempt', str(root), operation]
        run = subprocess.run(command, capture_output=True, text=True, timeout=60)
        if run.returncode:
            raise RuntimeError(f'traced probe exited {run.returncode}: {run.stderr[-3000:]}')
        outcome = json.loads(run.stdout)
        if outcome.get('operation') != operation or outcome.get('status') not in ('success', 'error'):
            raise ValueError('unexpected action outcome')
        lines = trace_path.read_text().splitlines()
        counts, eligible = base.traced_ordinals(lines, TRACED, root)
        injected = [line for line in lines if '(INJECTED)' in line]
        if len(injected) != (2 if second else 1):
            raise ValueError('unexpected number of injected EIOs')
        target = (targets_fixture_or_ancestor_sync(injected[0], syscall, root)
                  if all_sync_paths and syscall in SYNC
                  else base.targets_fixture(injected[0], syscall, root))
        if not re.search(r'\b' + syscall + r'\(', injected[0]) or not target:
            raise ValueError('primary EIO did not target the selected fixture call')
        if second and not re.search(r'\b' + second[0] + r'\(', injected[1]):
            raise ValueError('secondary EIO was absent or occurred before the primary EIO')
        if second and second[0] in CLEANUP and not base.targets_fixture(injected[1], second[0], root):
            raise ValueError('secondary removal did not target the fixture')
        return {'outcome': outcome, 'counts': counts, 'eligible_ordinals': eligible,
                'injected_traces': injected, 'calls_after_primary': calls_after_primary(
                    lines, syscall, root, allow_same_second=bool(second and second[0] == syscall))}
    return trace


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', required=True, help='existing disposable-fixture parent')
    parser.add_argument('--binary', default='target/release/examples/sync_failure_probe')
    parser.add_argument('--output', required=True, help='new JSON result path')
    parser.add_argument('--max-primary', type=int, default=0, help='limit each primary syscall; 0 means all')
    parser.add_argument('--primary-mode', choices=('data-namespace', 'sync', 'all'), default='data-namespace',
                        help='which successful-path calls may receive the first EIO')
    parser.add_argument('--all-sync-paths', action='store_true',
                        help='also select synchronization calls on resolved ancestor directories')
    parser.add_argument('--operation', choices=base.OPERATIONS, help='optional single-operation smoke check')
    parser.add_argument('--format', choices=base.FORMATS, help='optional single-format smoke check')
    args = parser.parse_args()
    if (not Path(args.directory).is_dir() or not Path(args.binary).is_file()
            or os.path.lexists(args.output) or not 0 <= args.max_primary <= 100
            or (args.all_sync_paths and args.primary_mode == 'data-namespace')):
        parser.error('existing fixture parent and binary, new output, and valid limit required')
    directory = str(Path(args.directory).resolve())
    if any(character in directory for character in ('"', '\\', '\n', '\r')):
        parser.error('fixture directory contains characters escaped by strace')
    binary = str(Path(args.binary).resolve())
    output = Path(args.output)
    result = {
        'revision': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
        'git_status': subprocess.check_output(['git', 'status', '--short'], text=True),
        'binary_sha256': base.digest(Path(binary).read_bytes()),
        'probe_sha256': base.digest(Path('examples/sync_failure_probe.rs').read_bytes()),
        'driver_sha256': base.digest(Path(__file__).read_bytes()),
        'common_driver_sha256': base.digest(Path(base.__file__).read_bytes()),
        'cargo_lock_sha256': base.digest(Path('Cargo.lock').read_bytes()),
        'strace': subprocess.check_output(['strace', '--version'], text=True).splitlines()[0],
        'host_platform': platform.platform(),
        'filesystem': json.loads(subprocess.check_output(
            ['findmnt', '-J', '-T', directory, '-o', 'TARGET,FSTYPE'], text=True)),
        'scope': ('For each selected data, namespace or sync call observed on the successful path, '
                  'inject one EIO. Discover sync and fixture-removal calls subsequently reached '
                  'on that failure path, then inject a second EIO at each selected later syscall '
                  'with each writable recovery order, including the same sync or removal syscall. '
                  'Data and namespace primary calls target the fixture; sync primary calls target '
                  'the fixture unless all_sync_paths is enabled, which also selects resolved ancestors. '
                  'This is a finite single-process model; it does not prove physical persistence.'),
        'max_primary': args.max_primary, 'primary_mode': args.primary_mode, 'all_sync_paths': args.all_sync_paths,
        'primary_syscalls': list({'data-namespace': DATA_NAMESPACE, 'sync': SYNC, 'all': PRIMARY}[args.primary_mode]),
        'formats': [args.format] if args.format else list(base.FORMATS),
        'operations': [args.operation] if args.operation else list(base.OPERATIONS),
        'started_utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'completed': False, 'failure': None, 'current': None, 'cases': [],
    }
    with output.open('x', encoding='utf-8') as file:
        json.dump(result, file, indent=2)
        file.write('\n')

    def save():
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', dir=output.parent,
                                         prefix='.double-eio-report-', delete=False) as file:
            temporary = Path(file.name)
            try:
                json.dump(result, file, indent=2)
                file.write('\n')
                file.flush()
                os.replace(temporary, output)
            finally:
                temporary.unlink(missing_ok=True)

    try:
        with tempfile.TemporaryDirectory(prefix='double-eio-campaign-', dir=directory) as temp:
            for format_name in result['formats']:
                for operation in result['operations']:
                    result['current'] = {'format': format_name, 'operation': operation, 'stage': 'control'}
                    save()
                    control = base.run_case(binary, temp, format_name, operation,
                                            syscalls=TRACED, fixture_only=True)
                    if control['outcome']['status'] != 'success':
                        raise ValueError('control action failed')
                    case = {'format': format_name, 'operation': operation, 'control': control,
                            'primary_checks': []}
                    result['cases'].append(case)
                    for syscall in result['primary_syscalls']:
                        ordinals = primary_ordinals(control, syscall, args.all_sync_paths)
                        if args.max_primary:
                            ordinals = ordinals[:args.max_primary]
                        for ordinal in ordinals:
                            result['current'] = {'format': format_name, 'operation': operation,
                                                 'syscall': syscall, 'ordinal': ordinal, 'stage': 'first'}
                            save()
                            first = base.run_case(binary, temp, format_name, operation, syscall, ordinal,
                                                  syscalls=TRACED, fixture_only=True,
                                                  tracer=tracer_for(all_sync_paths=args.all_sync_paths),
                                                  allow_unrecognized_markers=True)
                            check = {'syscall': syscall, 'ordinal': ordinal, 'first': first, 'second': []}
                            case['primary_checks'].append(check)
                            save()
                            for second_name in SECONDARY:
                                for second_ordinal in first['calls_after_primary'][second_name]:
                                    for order in base.RECOVERY_ORDERS:
                                        result['current'] = {'format': format_name, 'operation': operation,
                                                             'syscall': syscall, 'ordinal': ordinal,
                                                             'second_syscall': second_name,
                                                             'second_ordinal': second_ordinal,
                                                             'order': order, 'stage': 'second'}
                                        save()
                                        item = base.run_case(binary, temp, format_name, operation, syscall, ordinal,
                                                             order=order, syscalls=TRACED, fixture_only=True,
                                                             tracer=tracer_for((second_name, second_ordinal),
                                                                               all_sync_paths=args.all_sync_paths),
                                                             allow_unrecognized_markers=True)
                                        check['second'].append({'syscall': second_name, 'ordinal': second_ordinal,
                                                                'order': order, **item})
                                        save()
                    print(f'{format_name}/{operation}: {len(case["primary_checks"])} primary, '
                          f'{sum(len(item["second"]) for item in case["primary_checks"])} double failures',
                          flush=True)
        result['current'] = None
        result['completed'] = True
    except Exception as error:
        result['failure'] = f'{type(error).__name__}: {error}'
        raise
    finally:
        result['finished_utc'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
        save()
    print('double failures', sum(len(item['second']) for case in result['cases']
                                 for item in case['primary_checks']))


if __name__ == '__main__':
    main()
