import datetime
import hashlib
import json
import os
import pathlib
import platform
import subprocess
import tempfile
import time

CASES = {
    'unit': [
        'store::initialization_tests::killed_initialization_never_silently_reopens_as_legacy',
        'backup::publication_tests::process_kill_at_each_empty_legacy_restore_boundary_preserves_the_backup',
        'backup::publication_tests::process_kill_during_empty_legacy_restore_preserves_backup_and_recovers',
        'backup::publication_tests::empty_legacy_restore_fails_before_publication_at_each_new_boundary',
        'backup::publication_tests::empty_legacy_restore_header_handles_unsupported_and_failed_metadata',
    ],
    'integration': [
        'dangling_witness_entry_never_permits_creation_or_legacy_fallback',
        'empty_legacy_upgrade_respects_the_existing_store_lock',
        'every_incomplete_versioned_header_fails_closed_without_rewriting_it',
    ],
}
BINARIES = {name: pathlib.Path('/tmp/emda-empty-stress-' + name) for name in CASES}
def utc():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()
def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()
result = {
    'source_revision': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
    'source_status': subprocess.check_output(['git', 'status', '--short'], text=True).strip(),
    'started_utc': utc(),
    'kernel': platform.platform(),
    'executables_sha256': {name: sha(path) for name, path in BINARIES.items()},
    'repetitions_per_case_and_filesystem': 5,
    'runs': [],
    'limits': 'Process termination while the OS remains running is not physical power failure. The overlay backing storage is unknown. These tests do not constitute an off-host restore drill.',
}
for directory in ['/tmp', '/workspace']:
    mount = subprocess.check_output(['findmnt', '-no', 'TARGET,FSTYPE,SOURCE', '-T', directory], text=True).strip()
    with tempfile.TemporaryDirectory(prefix='emda-empty-stress-', dir=directory) as scratch:
        for kind, cases in CASES.items():
            for case in cases:
                elapsed=[]
                for repetition in range(5):
                    env=os.environ.copy()
                    env['TMPDIR']=scratch
                    env['RUST_TEST_THREADS']='1'
                    start=time.monotonic()
                    completed=subprocess.run([str(BINARIES[kind]), '--exact', case], env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=180)
                    elapsed.append(round(time.monotonic()-start, 6))
                    if completed.returncode or 'test result: ok. 1 passed;' not in completed.stdout:
                        print('FAIL', mount, case, repetition+1, completed.returncode, completed.stdout, flush=True)
                        raise SystemExit(1)
                result['runs'].append({'directory':directory,'mount':mount,'case':case,'repetitions':5,'elapsed_seconds':elapsed})
                print('PASS', directory, case, flush=True)
result['finished_utc']=utc()
pathlib.Path('/tmp/emda-empty-stress.json').write_text(json.dumps(result,indent=2)+'\n')
print('All',sum(run['repetitions'] for run in result['runs']),'runs passed',flush=True)
