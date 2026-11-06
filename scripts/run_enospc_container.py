#!/usr/bin/env python3
"""Run the ignored ENOSPC test only in a disposable, isolated tmpfs container."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import tempfile
import time
import uuid

IMAGE = 'debian@sha256:a72b4433de1cf7d6bae0c4cee74d0547f58601a643b3375f070c51600d63b6b2'
TEST = 'actual_tmpfs_enospc_preserves_source_and_recovery_evidence'


def cargo_binary(path):
    found = []
    for line in Path(path).read_text().splitlines():
        entry = json.loads(line)
        if (entry.get('reason') == 'compiler-artifact' and entry.get('target', {}).get('name') == 'enospc_tmpfs'
                and entry.get('profile', {}).get('test') and entry.get('executable')):
            found.append(entry['executable'])
    if len(found) != 1:
        raise ValueError('cargo output must identify exactly one enospc_tmpfs test executable')
    return Path(found[0])


def digest(path):
    value = hashlib.sha256()
    with Path(path).open('rb') as file:
        for block in iter(lambda: file.read(1024 * 1024), b''):
            value.update(block)
    return value.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument('--binary', help='prebuilt enospc_tmpfs test executable')
    source.add_argument('--cargo-json', help='JSON-lines output from cargo test --no-run')
    parser.add_argument('--output', required=True, help='new path for the recorded result')
    args = parser.parse_args()
    binary = Path(args.binary) if args.binary else cargo_binary(args.cargo_json)
    if not binary.is_file() or os.path.lexists(args.output):
        parser.error('a built test executable and a new output path are required')
    revision = subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()
    status = subprocess.check_output(['git', 'status', '--short'], text=True)
    name = 'emda-enospc-' + uuid.uuid4().hex
    report = {
        'revision': revision, 'git_status': status, 'binary_sha256': digest(binary),
        'test_source_sha256': digest('tests/enospc_tmpfs.rs'),
        'runner_sha256': digest(__file__), 'cargo_lock_sha256': digest('Cargo.lock'),
        'image': IMAGE, 'container_name': name, 'host_platform': platform.platform(),
        'started_utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'scope': 'Disposable 8 MiB tmpfs inside an isolated Docker container with a read-only root, no network, no capabilities, and only the test executable mounted read-only. A kernel ENOSPC result is not proof of physical disk, power-loss or off-host behavior.',
        'completed': False, 'failure': None,
    }
    output = Path(args.output)
    with output.open('x', encoding='utf-8') as file:
        json.dump(report, file, indent=2)
        file.write('\n')
    try:
        with tempfile.TemporaryDirectory(prefix='emda-enospc-runner-') as temp:
            mounted = Path(temp) / 'probe'
            shutil.copyfile(binary, mounted)
            mounted.chmod(0o755)
            command = [
                'docker', 'run', '--rm', '--pull=never', '--name', name, '--network', 'none',
                '--read-only', '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges',
                '--tmpfs', '/data:rw,size=8m,nosuid,nodev,noexec',
                '--mount', f'type=bind,src={mounted},dst=/probe,readonly',
                '--env', 'EMDA_ENOSPC_ROOT=/data', '--env', 'EMDA_ENOSPC_CONFIRM=disposable-tmpfs-only',
                IMAGE, '/probe', '--ignored', '--exact', TEST, '--nocapture',
            ]
            report['command'] = command
            run = subprocess.run(command, capture_output=True, text=True, timeout=120)
            report.update(exit_code=run.returncode, stdout=run.stdout, stderr=run.stderr)
            if run.returncode:
                raise RuntimeError(f'container test exited {run.returncode}')
            if '1 passed; 0 failed' not in run.stdout:
                raise RuntimeError('test did not report a passing ignored case')
            report['completed'] = True
    except Exception as error:
        report['failure'] = f'{type(error).__name__}: {error}'
        raise
    finally:
        # The random name identifies only the container created by this run.
        cleanup_error = None
        try:
            cleanup = subprocess.run(['docker', 'rm', '-f', name], capture_output=True, text=True, check=False, timeout=30)
            report['cleanup_exit_code'] = cleanup.returncode
            report['cleanup_stderr'] = cleanup.stderr[-1000:]
        except (OSError, subprocess.TimeoutExpired) as error:
            cleanup_error = error
            report['completed'] = False
            report['cleanup_error'] = f'{type(error).__name__}: {error}'
        report['finished_utc'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
        output.write_text(json.dumps(report, indent=2) + '\n')
        if cleanup_error:
            raise RuntimeError(f'failed to clean up disposable container {name}') from cleanup_error
    print(report['stderr'].strip())
    print(report['stdout'].strip())


if __name__ == '__main__':
    main()
