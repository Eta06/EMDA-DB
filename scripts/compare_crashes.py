#!/usr/bin/env python3
"""Kill disposable database processes after acknowledged mutations and reopen.

This is a process-crash check, not a power-loss test or proof of durability.
"""
import argparse
import contextlib
import json
import os
from pathlib import Path
import secrets
import selectors
import subprocess
import sys
import tempfile

from benchmarks.clients import AdapterClient, PostgresClient, RedisClient, MongoClient, FoundationClient
from benchmarks.services import IMAGES, LocalService
from compare_engines import BACKENDS


def adapter(backend, directory, server):
    command = ([server] if backend in ("prototype", "sqlite") else [sys.executable, "scripts/embedded_bench_server.py"])
    process = subprocess.Popen(command + [backend, str(directory)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(process.stdout, selectors.EVENT_READ)
            if not selector.select(20):
                raise RuntimeError("adapter startup timeout")
            ready = process.stdout.readline().split()
        if len(ready) != 2 or ready[0] != "READY":
            raise RuntimeError("adapter did not start")
        return process, AdapterClient(int(ready[1]))
    except BaseException:
        process.kill()
        process.wait()
        raise


def check(backend, parent, server):
    table = "emda_crash_" + secrets.token_hex(8)
    prefix = (table + ":").encode()
    service = None
    process = None
    client = None
    original_env = {}
    with tempfile.TemporaryDirectory(prefix=".crash-check-", dir=parent) as directory:
        try:
            if backend in IMAGES:
                service = LocalService(backend, parent)
                service.start()
                for key, value in service.env.items():
                    original_env[key] = os.environ.get(key)
                    os.environ[key] = value
                def connect():
                    if backend == "postgres": return PostgresClient(table)
                    if backend == "redis": return RedisClient(prefix)
                    if backend == "mongo": return MongoClient(table)
                    return FoundationClient(prefix)
                client = connect()
                if backend == "postgres":
                    for key in ("fsync", "synchronous_commit", "full_page_writes"):
                        assert client.conn.execute("SHOW " + key).fetchone()[0] == "on"
                    client.conn.execute(f"CREATE TABLE {table} (key BYTEA PRIMARY KEY, value BYTEA NOT NULL, checksum BIGINT NOT NULL)")
                elif backend == "redis":
                    assert client.conn.config_get("appendfsync")["appendfsync"] == "always"
                metadata = service.metadata()
            else:
                process, client = adapter(backend, directory, server)
                metadata = json.loads(client.request(b"M", b"_"))
            expected = {}
            for i in range(100):
                key, value = i.to_bytes(8, "big"), bytes([i]) * 64
                client.put(key, value)
                expected[key] = value
            for i in range(10):
                key, value = i.to_bytes(8, "big"), bytes([255 - i]) * 64
                client.put(key, value)
                expected[key] = value
            for i in range(90, 100):
                key = i.to_bytes(8, "big")
                client.delete(key)
                del expected[key]
            # Keep client connections open until the server is killed. Closing
            # the last SQLite connection first could checkpoint its WAL.
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
            if service:
                service.start()
                os.environ.update(service.env)
                client = connect()
            else:
                process, client = adapter(backend, directory, server)
            for key, value in expected.items():
                if client.get(key) != value:
                    raise RuntimeError("acknowledged value missing or incorrect after restart")
            for i in range(90, 100):
                if client.get(i.to_bytes(8, "big")) is not None:
                    raise RuntimeError("acknowledged deletion did not survive restart")
            return {"backend": backend, "acknowledged_mutations": 120, "verified_values": 90,
                    "verified_deletions": 10, "result": "passed", "metadata": metadata}
        finally:
            if client:
                with contextlib.suppress(Exception): client.close()
            if process:
                process.kill()
                process.wait(timeout=10)
                process.stdout.close()
                process.stderr.close()
            if service:
                service.close()
            for key, old in original_env.items():
                if old is None: os.environ.pop(key, None)
                else: os.environ[key] = old


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--server", default="target/release/examples/network_bench_server")
    parser.add_argument("--backends", nargs="+", choices=BACKENDS, default=BACKENDS)
    args = parser.parse_args()
    result = {"revision": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
              "scope": "SIGKILL after acknowledged writes, updates and deletes; no physical power failure", "runs": []}
    for backend in args.backends:
        try:
            outcome = check(backend, args.directory, args.server)
        except Exception as error:
            outcome = {"backend": backend, "result": "failed", "error": str(error)}
        result["runs"].append(outcome)
        Path(args.output).write_text(json.dumps(result, indent=2) + "\n")
        print(backend, outcome["result"], flush=True)
    if any(run["result"] != "passed" for run in result["runs"]):
        raise SystemExit("One or more crash checks failed; see results")


if __name__ == "__main__":
    main()
