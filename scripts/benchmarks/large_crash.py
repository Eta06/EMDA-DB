"""Seed and verify large, disposable cross-engine process-crash fixtures."""
import contextlib
import json
import os
from pathlib import Path
import secrets
import subprocess
import tempfile
import time
import zlib

from benchmarks.clients import MongoClient, PostgresClient
from benchmarks.services import LocalService, docker
from benchmarks.telemetry import difference, snapshot
from compare_crashes import adapter
from index_memory_compare import fields

BACKENDS = ('prototype', 'sqlite', 'postgres', 'mongo')


def key(number):
    return number.to_bytes(8, 'big')


def value(number):
    return bytes([number % 251]) * 64


def updated(number):
    return bytes([255 - number]) * 64


def seed_service(client, backend, count):
    """Commit identical values in bounded batches; preparation is not timed."""
    if backend == 'postgres':
        settings = {name: client.conn.execute('SHOW ' + name).fetchone()[0]
                    for name in ('fsync', 'synchronous_commit', 'full_page_writes')}
        if any(item != 'on' for item in settings.values()):
            raise RuntimeError('PostgreSQL requires fsync, synchronous commit and full page writes')
        client.conn.execute(f'CREATE TABLE {client.table} '
                            '(key BYTEA PRIMARY KEY, value BYTEA NOT NULL, checksum BIGINT NOT NULL)')
        for start in range(0, count, 512):
            rows = [(key(index), value(index), zlib.crc32(value(index)))
                    for index in range(start, min(start + 512, count))]
            with client.conn.transaction(), client.conn.cursor() as cursor:
                cursor.executemany(f'INSERT INTO {client.table} (key,value,checksum) VALUES (%s,%s,%s)', rows)
        return {'settings': settings, 'version': client.conn.info.server_version}
    if backend == 'mongo':
        concern = client.collection.write_concern.document
        if concern.get('w') != 1 or concern.get('j') is not True:
            raise RuntimeError('MongoDB requires w=1 and journal acknowledgement')
        for start in range(0, count, 512):
            rows = [{'_id': key(index), 'value': value(index), 'checksum': zlib.crc32(value(index))}
                    for index in range(start, min(start + 512, count))]
            client.collection.insert_many(rows, ordered=True)
        return {'write_concern': concern, 'version': client.conn.server_info()['version']}
    raise ValueError('unsupported service')


def prepare_embedded(backend, parent, count, prototype_preparer, sqlite_preparer):
    binary = prototype_preparer if backend == 'prototype' else sqlite_preparer
    result = subprocess.run([binary, 'prepare', str(parent), str(count), '8', '0'],
                            check=True, capture_output=True, text=True, timeout=600)
    output = fields(result.stdout) if backend == 'prototype' else json.loads(result.stdout)
    if (int(output.get('keys', -1)) != count or int(output.get('key_bytes', -1)) != 8
            or int(output.get('tail_keys', -1)) != 0):
        raise ValueError('fixture preparation returned unexpected sizes')
    if backend == 'sqlite' and (output.get('journal') != 'wal' or output.get('synchronous') != 'FULL'):
        raise ValueError('SQLite fixture did not use WAL and FULL synchronization')
    return output


def apply_changes(client, count):
    for index in range(100):
        client.put(key(count + index), value(count + index))
    for index in range(10):
        client.put(key(count + index), updated(index))
    for index in range(90, 100):
        client.delete(key(count + index))


def verify(client, count):
    """Check all seeded values and every later insert, update and deletion."""
    for index in range(count):
        if client.get(key(index)) != value(index):
            raise ValueError(f'seeded key {index} missing or incorrect after restart')
    for index in range(90):
        expected = updated(index) if index < 10 else value(count + index)
        if client.get(key(count + index)) != expected:
            raise ValueError(f'acknowledged mutation {index} missing or incorrect after restart')
    for index in range(90, 100):
        if client.get(key(count + index)) is not None:
            raise ValueError(f'acknowledged deletion {index} missing after restart')
    return {'seeded_values': count, 'later_values': 90, 'deletions': 10}


def process_resources(pid):
    """Read one process only; PostgreSQL children and page cache are excluded."""
    result = {'pid': pid}
    try:
        for line in Path(f'/proc/{pid}/status').read_text().splitlines():
            if line.startswith(('VmRSS:', 'VmHWM:')):
                name, amount, unit = line.split()
                if unit != 'kB':
                    raise ValueError('unexpected /proc memory unit')
                result[name.rstrip(':') + '_kib'] = int(amount)
        for line in Path(f'/proc/{pid}/io').read_text().splitlines():
            name, amount = line.split()
            if name in ('read_bytes:', 'write_bytes:', 'syscr:', 'syscw:'):
                result[name.rstrip(':')] = int(amount)
    except (OSError, ValueError) as error:
        result['error'] = f'{type(error).__name__}: {error}'
    return result


def server_resources(service, process):
    if not service:
        return {'scope': 'adapter process and its threads only', 'process': process_resources(process.pid)}
    try:
        pid = int(json.loads(docker('inspect', service.name))[0]['State']['Pid'])
        stats = json.loads(docker('stats', '--no-stream', '--format', '{{json .}}', service.name, timeout=30))
        return {'scope': 'container snapshot and main process only; PostgreSQL children excluded from process data',
                'process': process_resources(pid),
                'container': {name: stats.get(name) for name in ('MemUsage', 'MemPerc', 'BlockIO', 'PIDs')}}
    except (OSError, ValueError, KeyError, IndexError, subprocess.SubprocessError) as error:
        return {'scope': 'container resource sample unavailable', 'error': service.redact(str(error))}


def fixture_bytes(parent):
    total = 0
    files = 0
    for path in parent.rglob('*'):
        if path.is_file() and not path.is_symlink():
            total += path.stat().st_size
            files += 1
    return {'regular_files': files, 'logical_file_bytes': total}


def service_fixture_bytes(service):
    # The service may own inaccessible directories (notably PostgreSQL's
    # PGDATA). Ask the same disposable container to measure its own mount.
    try:
        output = docker('exec', service.name, 'du', '-sb', '/data')
        amount = int(output.split()[0])
        return {'apparent_bytes_including_directory_entries': amount, 'method': 'du -sb /data'}
    except (OSError, ValueError, IndexError, subprocess.SubprocessError) as error:
        return {'error': service.redact(str(error))}


def check(backend, parent, server, prototype_preparer, sqlite_preparer, count, cpus):
    if backend not in BACKENDS:
        raise ValueError('unsupported backend')
    table = 'emda_large_' + secrets.token_hex(8)
    service = None
    process = None
    client = None
    original_env = {}
    with tempfile.TemporaryDirectory(prefix='.large-crash-', dir=parent) as directory:
        fixture = Path(directory)
        try:
            if backend in ('postgres', 'mongo'):
                service = LocalService(backend, parent, cpus)
                service.start()
                for name, setting in service.env.items():
                    original_env[name] = os.environ.get(name)
                    os.environ[name] = setting
                connect = (lambda: PostgresClient(table)) if backend == 'postgres' else (lambda: MongoClient(table))
                client = connect()
                preparation = seed_service(client, backend, count)
                metadata = service.metadata()
            else:
                preparation = prepare_embedded(backend, fixture, count, prototype_preparer, sqlite_preparer)
                process, client = adapter(backend, fixture, server, cpus)
                metadata = json.loads(client.request(b'M', b'_'))
                if backend == 'prototype':
                    with (fixture / 'store' / 'records.log').open('rb') as log:
                        metadata['log_magic'] = log.read(8).decode('ascii')
            apply_changes(client, count)
            storage = service_fixture_bytes(service) if service else fixture_bytes(fixture)
            if service:
                service.kill()
            else:
                process.kill()
                process.wait(timeout=10)
                process.stdout.close()
                process.stderr.close()
                process = None
            client.close()
            client = None
            before = snapshot()
            started = time.perf_counter_ns()
            if service:
                service.start()
                os.environ.update(service.env)
                client = connect()
            else:
                process, client = adapter(backend, fixture, server, cpus)
            ready_ns = time.perf_counter_ns() - started
            noise = difference(before, snapshot())
            resources = server_resources(service, process)
            started = time.perf_counter_ns()
            verified = verify(client, count)
            verify_ns = time.perf_counter_ns() - started
            return {'backend': backend, 'result': 'passed', 'prepared': preparation,
                    'acknowledged_mutations_after_seed': 120, 'verified': verified,
                    'restart_ready_ns': ready_ns, 'verification_ns': verify_ns,
                    'restart_noise': noise, 'resources_after_ready': resources,
                    'fixture_before_kill': storage, 'metadata': metadata}
        except Exception as error:
            # PostgreSQL connection diagnostics can contain its generated password.
            if service:
                raise RuntimeError(service.redact(f'{type(error).__name__}: {error}')) from None
            raise
        finally:
            if client:
                with contextlib.suppress(Exception):
                    client.close()
            if process:
                process.kill()
                process.wait(timeout=10)
                process.stdout.close()
                process.stderr.close()
            try:
                if service:
                    service.close()
            finally:
                for name, previous in original_env.items():
                    if previous is None:
                        os.environ.pop(name, None)
                    else:
                        os.environ[name] = previous
