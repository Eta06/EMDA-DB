#!/usr/bin/env python3
"""Kill disposable database processes after acknowledged mutations and reopen.

This is a process-crash check, not a power-loss test or proof of durability.
"""
import argparse
import contextlib
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import secrets
import selectors
import subprocess
import sys
import tempfile
import time

from benchmarks.clients import AdapterClient, PostgresClient, RedisClient, MongoClient, FoundationClient
from benchmarks.services import IMAGES, LocalService
from compare_engines import BACKENDS


def adapter(backend, directory, server, cpus=None):
    command = ([server] if backend in ("prototype", "sqlite") else [sys.executable, "scripts/embedded_bench_server.py"])
    if cpus is not None:
        command = ["taskset", "-c", cpus, *command]
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
                    settings = {key: client.conn.execute("SHOW " + key).fetchone()[0]
                                for key in ("fsync", "synchronous_commit", "full_page_writes")}
                    if any(value != "on" for value in settings.values()):
                        raise RuntimeError("PostgreSQL durable settings are required")
                    client.conn.execute(f"CREATE TABLE {table} (key BYTEA PRIMARY KEY, value BYTEA NOT NULL, checksum BIGINT NOT NULL)")
                elif backend == "redis":
                    if client.conn.config_get("appendfsync")["appendfsync"] != "always":
                        raise RuntimeError("Redis appendfsync=always is required")
                metadata = service.metadata()
                if backend == "postgres":
                    metadata.update(server_version=client.conn.info.server_version, settings=settings)
                elif backend == "redis":
                    metadata.update(server_version=client.conn.info("server")["redis_version"], appendfsync="always")
                elif backend == "mongo":
                    metadata.update(server_version=client.conn.server_info()["version"],
                                    write_concern=client.collection.write_concern.document)
            else:
                process, client = adapter(backend, directory, server)
                metadata = json.loads(client.request(b"M", b"_"))
                if backend == "prototype":
                    with (Path(directory) / "store" / "records.log").open("rb") as source:
                        metadata["log_magic"] = source.read(8).decode("ascii")
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
    parser.add_argument("--rounds", type=int, default=1, help="fresh fixtures per backend, 1 to 10")
    args = parser.parse_args()
    output = Path(args.output)
    if (not 1 <= args.rounds <= 10 or len(set(args.backends)) != len(args.backends)
            or not Path(args.directory).is_dir() or not Path(args.server).is_file()
            or not output.parent.is_dir() or output.exists()):
        parser.error("invalid rounds, backends, directory, server, or existing output")
    sources = sorted([Path(__file__), Path("scripts/compare_engines.py"), *Path("scripts/benchmarks").glob("*.py"),
                      Path("scripts/embedded_bench_server.py"), *Path("examples").rglob("*.rs"),
                      *Path("src").rglob("*.rs"), Path("Cargo.toml"), Path("Cargo.lock")])

    def digest(path):
        h = hashlib.sha256()
        with open(path, "rb") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                h.update(chunk)
        return h.hexdigest()

    versions = {}
    for package in ("psycopg", "redis", "pymongo", "pyturso", "rocksdict", "lmdb", "duckdb", "foundationdb"):
        try:
            versions[package] = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            versions[package] = None
    result = {
        "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "revision": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "git_status": subprocess.check_output(["git", "status", "--short"], text=True),
        "source_sha256": {str(path): digest(path) for path in sources},
        "server_sha256": digest(args.server), "python": sys.version, "platform": platform.platform(),
        "dependencies": versions,
        "filesystem": json.loads(subprocess.check_output(
            ["findmnt", "-J", "-T", args.directory, "-o", "TARGET,FSTYPE"], text=True)),
        "scope": "SIGKILL after acknowledged writes, updates and deletes; no physical power failure",
        "rounds": args.rounds, "backends": args.backends, "completed": False, "current": None, "runs": [],
    }

    def save():
        # Keep the last complete result even if this process is interrupted.
        with tempfile.NamedTemporaryFile(mode="w", dir=output.parent, prefix="." + output.name + ".",
                                         delete=False) as temporary:
            name = temporary.name
            try:
                json.dump(result, temporary, indent=2)
                temporary.write("\n")
            except BaseException:
                os.unlink(name)
                raise
        try:
            os.replace(name, output)
        finally:
            if os.path.exists(name):
                os.unlink(name)

    save()
    for round_index in range(args.rounds):
        rotation = round_index % len(args.backends)
        for backend in args.backends[rotation:] + args.backends[:rotation]:
            result["current"] = {"round": round_index + 1, "backend": backend}
            save()
            try:
                outcome = check(backend, args.directory, args.server)
            except Exception as error:
                outcome = {"backend": backend, "result": "failed", "error": str(error)}
            result["runs"].append({"round": round_index + 1, **outcome})
            save()
            print(f"round={round_index + 1} {backend} {outcome['result']}", flush=True)
    result["current"] = None
    result["completed"] = True
    result["finished_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    save()
    if any(run["result"] != "passed" for run in result["runs"]):
        raise SystemExit("One or more crash checks failed; see results")


if __name__ == "__main__":
    main()
