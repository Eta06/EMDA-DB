#!/usr/bin/env python3
"""Closed-loop, loopback key-value comparison; see docs/BENCHMARKS.md.

Native servers must be disposable local instances configured as documented.
Only uniquely named tables and keys created by this process are removed.
"""
import argparse
import concurrent.futures
import contextlib
import json
import math
import os
import random
import secrets
import selectors
import subprocess
import tempfile
import threading
import time
import sys
from pathlib import Path


from benchmarks.clients import AdapterClient, PostgresClient, RedisClient, MongoClient, FoundationClient
from benchmarks.telemetry import snapshot, difference, parse_cpus


def latency(samples):
    if not samples:
        return {"count": 0}
    values = sorted(samples)
    def percentile(p):
        return round(values[math.ceil(len(values) * p) - 1] / 1000, 1)
    return {"count": len(values), "p50_us": percentile(.5), "p95_us": percentile(.95),
            "p99_us": percentile(.99), "p99_9_us": percentile(.999), "max_us": round(values[-1] / 1000, 1)}


def adapter_metadata(client):
    try:
        return json.loads(client.request(b"M", b"_"))
    except RuntimeError as error:
        # Historical Rust adapters predate metadata discovery. Keep the
        # existing before/after latency harness usable with those binaries.
        if str(error) == "invalid benchmark request":
            return {"adapter_metadata": "unsupported by this historical binary"}
        raise


def run(args):
    run_id = secrets.token_hex(8)
    table = "emda_bench_" + run_id
    prefix = ("emda_bench_" + run_id + ":").encode()
    keys = [i.to_bytes(8, "big") for i in range(args.keys)]
    clients = []
    admin = None
    server = None
    directory = None
    metadata = {}
    python_backends = ("sqlite_python", "turso", "lmdb", "rocksdb", "duckdb")
    adapter_backends = ("prototype", "sqlite") + python_backends
    try:
        if args.backend in adapter_backends:
            directory = tempfile.TemporaryDirectory(prefix=".network-bench-", dir=args.directory)
            command = ([sys.executable, str(Path(__file__).with_name("embedded_bench_server.py"))]
                       if args.backend in python_backends else [args.server])
            if getattr(args, "server_cpus", None):
                command = ["taskset", "-c", args.server_cpus] + command
            server = subprocess.Popen(command + [args.backend, directory.name], stdout=subprocess.PIPE,
                                      stderr=subprocess.PIPE, text=True, bufsize=1)
            with selectors.DefaultSelector() as selector:
                selector.register(server.stdout, selectors.EVENT_READ)
                if not selector.select(20):
                    raise RuntimeError("benchmark server did not become ready")
                ready = server.stdout.readline().strip().split()
            if len(ready) != 2 or ready[0] != "READY":
                raise RuntimeError("benchmark server failed to start: " + " ".join(ready))
            clients = [AdapterClient(int(ready[1])) for _ in range(args.clients)]
            metadata["transport"] = "TCP loopback to " + ("Python binding adapter" if args.backend in python_backends else "Rust adapter")
            metadata.update(adapter_metadata(clients[0]))
        elif args.backend == "postgres":
            import psycopg
            admin = PostgresClient(table)
            settings = {key: admin.conn.execute("SHOW " + key).fetchone()[0]
                        for key in ("fsync", "synchronous_commit", "full_page_writes")}
            if any(value != "on" for value in settings.values()):
                raise RuntimeError(f"PostgreSQL durable settings required: {settings}")
            metadata.update(settings)
            metadata["wal_sync_method"] = admin.conn.execute("SHOW wal_sync_method").fetchone()[0]
            metadata["version"] = admin.conn.execute("SELECT version()").fetchone()[0]
            metadata["driver_version"] = psycopg.__version__
            metadata["transport"] = "TCP loopback through container port forwarding"
            admin.conn.execute(f"CREATE TABLE {table} (key BYTEA PRIMARY KEY, value BYTEA NOT NULL, checksum BIGINT NOT NULL)")
            clients = [PostgresClient(table) for _ in range(args.clients)]
        elif args.backend == "redis":
            import redis
            admin = RedisClient(prefix)
            settings = {key: admin.conn.config_get(key).get(key) for key in ("appendonly", "appendfsync")}
            if settings != {"appendonly": "yes", "appendfsync": "always"}:
                raise RuntimeError(f"Redis AOF with appendfsync=always required: {settings}")
            metadata.update(settings)
            if admin.conn.config_get("no-appendfsync-on-rewrite").get("no-appendfsync-on-rewrite") != "no":
                raise RuntimeError("Redis must not skip sync during AOF rewrites")
            metadata["version"] = admin.conn.info("server")["redis_version"]
            metadata["driver_version"] = redis.__version__
            metadata["transport"] = "TCP loopback through container port forwarding"
            clients = [RedisClient(prefix) for _ in range(args.clients)]
        elif args.backend == "mongo":
            import pymongo
            admin = MongoClient(table)
            metadata.update(version=admin.conn.server_info()["version"], driver_version=pymongo.version,
                            write_concern=admin.collection.write_concern.document,
                            configuration=admin.conn.admin.command("getCmdLineOpts").get("parsed", {}),
                            transport="MongoDB wire protocol over container port forwarding")
            if metadata["write_concern"].get("j") is not True:
                raise RuntimeError("journal acknowledgement required")
            clients = [MongoClient(table) for _ in range(args.clients)]
        elif args.backend == "foundationdb":
            import importlib.metadata
            admin = FoundationClient(prefix)
            metadata.update(driver_version=importlib.metadata.version("foundationdb"), api_version=730,
                            durability="default durable transaction commit; single-process single-replica test cluster",
                            transport="native FoundationDB protocol over container port forwarding")
            clients = [FoundationClient(prefix) for _ in range(args.clients)]
        else:
            raise ValueError("unknown backend")

        if getattr(args, "server_cpus", None):
            metadata["requested_server_cpus"] = sorted(parse_cpus(args.server_cpus))
        initial = bytes(args.value_bytes)
        for key in keys:
            clients[0].put(key, initial)
        for key in keys[:100]:
            if clients[0].get(key) != initial:
                raise RuntimeError("warm-up validation failed")
        barrier = threading.Barrier(args.clients + 1)
        workload = getattr(args, "workload", "mixed")

        def worker(index):
            rng = random.Random(0xDB001 + index)
            client = clients[index]
            reads, writes, creates, updates, deletes = [], [], [], [], []
            barrier.wait()
            if workload == "crud":
                # Each client owns its key. Timed create/read/update/delete;
                # verify the updated value and absence outside operation timers.
                key = b"crud" + index.to_bytes(4, "big")
                for sequence in range(args.operations):
                    first = bytes([sequence % 256]) * args.value_bytes
                    second = bytes([(sequence + 1) % 256]) * args.value_bytes
                    for name, action in [("create", lambda: client.put(key, first)),
                                         ("read", lambda: client.get(key)),
                                         ("update", lambda: client.put(key, second))]:
                        started = time.perf_counter_ns()
                        value = action()
                        elapsed = time.perf_counter_ns() - started
                        {"create": creates, "read": reads, "update": updates}[name].append(elapsed)
                        if name == "read" and value != first:
                            raise RuntimeError("CRUD read returned a stale or corrupt value")
                    if client.get(key) != second:
                        raise RuntimeError("CRUD update verification failed")
                    started = time.perf_counter_ns()
                    client.delete(key)
                    deletes.append(time.perf_counter_ns() - started)
                    if client.get(key) is not None:
                        raise RuntimeError("CRUD delete verification failed")
                return reads, writes, creates, updates, deletes
            for _ in range(args.operations):
                write = rng.randrange(100) < args.write_percent
                key = keys[rng.randrange(args.keys)]
                value = bytes([rng.randrange(256)]) * args.value_bytes
                started = time.perf_counter_ns()
                if write:
                    client.put(key, value)
                    writes.append(time.perf_counter_ns() - started)
                else:
                    found = client.get(key)
                    reads.append(time.perf_counter_ns() - started)
                    if len(found) != args.value_bytes or any(byte != found[0] for byte in found):
                        raise RuntimeError("invalid read result")
            return reads, writes, creates, updates, deletes

        with concurrent.futures.ThreadPoolExecutor(max_workers=args.clients) as pool:
            futures = [pool.submit(worker, index) for index in range(args.clients)]
            before = snapshot()
            started = time.perf_counter_ns()
            barrier.wait()
            outputs = [future.result() for future in futures]
            elapsed = time.perf_counter_ns() - started
            noise = difference(before, snapshot())
        if args.backend in adapter_backends:
            metadata.update(adapter_metadata(clients[0]))
        for key in keys:
            value = clients[0].get(key)
            if len(value) != args.value_bytes or any(byte != value[0] for byte in value):
                raise RuntimeError("post-run value verification failed")
        reads = [sample for output in outputs for sample in output[0]]
        writes = [sample for output in outputs for sample in output[1]]
        creates = [sample for output in outputs for sample in output[2]]
        updates = [sample for output in outputs for sample in output[3]]
        deletes = [sample for output in outputs for sample in output[4]]
        completed = sum(map(len, (reads, writes, creates, updates, deletes)))
        return {"backend": args.backend, "workload": workload, "noise": noise, "clients": args.clients, "operations_per_client": args.operations * (4 if workload == "crud" else 1),
                "crud_cycles_per_client": args.operations if workload == "crud" else None,
                "keys": args.keys, "write_percent": args.write_percent, "value_bytes": args.value_bytes,
                "completed": completed, "elapsed_ms": round(elapsed / 1e6, 2),
                "ops_per_sec": round(completed * 1e9 / elapsed, 1),
                "read": latency(reads), "write": latency(writes), "create": latency(creates),
                "update": latency(updates), "delete": latency(deletes), "metadata": metadata,
                "throughput_note": "CRUD wall time includes two untimed verification reads per cycle" if workload == "crud" else "normal closed-loop operations"}
    finally:
        for client in clients:
            with contextlib.suppress(Exception):
                client.close()
        try:
            if admin is not None:
                try:
                    if args.backend == "postgres":
                        admin.conn.execute(f"DROP TABLE IF EXISTS {table}")
                    elif args.backend == "redis":
                        admin.conn.delete(*(prefix + key for key in keys), *(prefix + b"crud" + i.to_bytes(4, "big") for i in range(args.clients)))
                    elif args.backend == "mongo":
                        admin.collection.drop()
                    elif args.backend == "foundationdb":
                        admin.conn.clear_range_startswith(prefix)
                finally:
                    admin.close()
        finally:
            if server is not None:
                server.terminate()
                try:
                    server.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    server.kill()
                    server.wait()
                server.stdout.close()
                server.stderr.close()
            if directory is not None:
                directory.cleanup()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("backend", choices=("prototype", "sqlite", "postgres", "redis", "mongo", "foundationdb", "sqlite_python", "turso", "lmdb", "rocksdb", "duckdb"))
    parser.add_argument("--workload", choices=("mixed", "crud"), default="mixed")
    parser.add_argument("--client-cpus", help="Linux CPU affinity for benchmark client threads")
    parser.add_argument("--server-cpus", help="Linux CPU affinity for the spawned adapter")
    parser.add_argument("--server", default="target/release/examples/network_bench_server")
    parser.add_argument("--directory", default=".")
    parser.add_argument("--clients", type=int, default=4)
    parser.add_argument("--operations", type=int, default=1000)
    parser.add_argument("--keys", type=int, default=250)
    parser.add_argument("--write-percent", type=int, default=50)
    parser.add_argument("--value-bytes", type=int, default=64)
    args = parser.parse_args()
    if not (1 <= args.clients <= 64 and 1 <= args.operations <= 100000 and 1 <= args.keys <= 100000
            and 0 <= args.write_percent <= 100 and 1 <= args.value_bytes <= 16 * 1024 * 1024
            and os.path.isdir(args.directory)):
        parser.error("invalid workload limits or nonexistent directory")
    if args.client_cpus:
        os.sched_setaffinity(0, parse_cpus(args.client_cpus))
    print(json.dumps(run(args), sort_keys=True))


if __name__ == "__main__":
    main()
