#!/usr/bin/env python3
"""Closed-loop, loopback key-value comparison; see docs/BENCHMARKS.md.

PostgreSQL and Redis must be disposable local servers configured as documented.
Only uniquely named tables and keys created by this process are removed.
"""
import argparse
import concurrent.futures
import json
import math
import os
import random
import secrets
import selectors
import socket
import struct
import subprocess
import tempfile
import threading
import time
import zlib


def exact(sock, size):
    parts = bytearray()
    while len(parts) < size:
        data = sock.recv(size - len(parts))
        if not data:
            raise RuntimeError("benchmark server closed the connection")
        parts.extend(data)
    return bytes(parts)


class AdapterClient:
    def __init__(self, port):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=30)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    def request(self, operation, key, value=b""):
        self.sock.sendall(struct.pack(">cII", operation, len(key), len(value)) + key + value)
        status, size = struct.unpack(">BI", exact(self.sock, 5))
        data = exact(self.sock, size)
        if status:
            raise RuntimeError(data.decode("utf-8", "replace"))
        return data

    def get(self, key):
        return self.request(b"R", key)

    def put(self, key, value):
        self.request(b"W", key, value)

    def close(self):
        self.sock.close()


class PostgresClient:
    def __init__(self, table):
        import psycopg
        self.table = table  # Generated locally from a fixed prefix and hex digits.
        self.conn = psycopg.connect(
            host="127.0.0.1", port=int(os.environ["EMDA_BENCH_PG_PORT"]),
            dbname="benchmark", user="postgres", password=os.environ["EMDA_BENCH_PG_PASSWORD"],
            autocommit=True, prepare_threshold=0, connect_timeout=10,
        )

    def get(self, key):
        row = self.conn.execute(f"SELECT value, checksum FROM {self.table} WHERE key = %s", (key,)).fetchone()
        if row is None:
            raise RuntimeError("missing PostgreSQL key")
        value, checksum = bytes(row[0]), row[1]
        if zlib.crc32(value) != checksum:
            raise RuntimeError("PostgreSQL value checksum mismatch")
        return value

    def put(self, key, value):
        self.conn.execute(
            f"INSERT INTO {self.table} (key, value, checksum) VALUES (%s, %s, %s) "
            "ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value, checksum=EXCLUDED.checksum",
            (key, value, zlib.crc32(value)),
        )

    def close(self):
        self.conn.close()


class RedisClient:
    def __init__(self, prefix):
        import redis
        self.prefix = prefix
        self.conn = redis.Redis(host="127.0.0.1", port=int(os.environ["EMDA_BENCH_REDIS_PORT"]), socket_timeout=30)

    def get(self, key):
        payload = self.conn.get(self.prefix + key)
        if payload is None or len(payload) < 4:
            raise RuntimeError("missing or incomplete Redis value")
        value, checksum = payload[:-4], int.from_bytes(payload[-4:], "big")
        if zlib.crc32(value) != checksum:
            raise RuntimeError("Redis value checksum mismatch")
        return value

    def put(self, key, value):
        self.conn.set(self.prefix + key, value + zlib.crc32(value).to_bytes(4, "big"))

    def close(self):
        self.conn.close()


def latency(samples):
    if not samples:
        return {"count": 0}
    values = sorted(samples)
    def percentile(p):
        return round(values[math.ceil(len(values) * p) - 1] / 1000, 1)
    return {"count": len(values), "p50_us": percentile(.5), "p95_us": percentile(.95),
            "p99_us": percentile(.99), "p99_9_us": percentile(.999), "max_us": round(values[-1] / 1000, 1)}


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
    try:
        if args.backend in ("prototype", "sqlite"):
            directory = tempfile.TemporaryDirectory(prefix=".network-bench-", dir=args.directory)
            server = subprocess.Popen([args.server, args.backend, directory.name], stdout=subprocess.PIPE,
                                      stderr=subprocess.PIPE, text=True, bufsize=1)
            with selectors.DefaultSelector() as selector:
                selector.register(server.stdout, selectors.EVENT_READ)
                if not selector.select(20):
                    raise RuntimeError("benchmark server did not become ready")
                ready = server.stdout.readline().strip().split()
            if len(ready) != 2 or ready[0] != "READY":
                raise RuntimeError("benchmark server failed to start: " + " ".join(ready))
            clients = [AdapterClient(int(ready[1])) for _ in range(args.clients)]
            metadata["transport"] = "TCP loopback to native benchmark adapter"
        elif args.backend == "postgres":
            import psycopg
            admin = PostgresClient(table)
            settings = {key: admin.conn.execute("SHOW " + key).fetchone()[0]
                        for key in ("fsync", "synchronous_commit", "full_page_writes")}
            if any(value != "on" for value in settings.values()):
                raise RuntimeError(f"PostgreSQL durable settings required: {settings}")
            metadata.update(settings)
            metadata["version"] = admin.conn.execute("SELECT version()").fetchone()[0]
            metadata["driver_version"] = psycopg.__version__
            metadata["transport"] = "TCP loopback through container port forwarding"
            admin.conn.execute(f"CREATE TABLE {table} (key BYTEA PRIMARY KEY, value BYTEA NOT NULL, checksum BIGINT NOT NULL)")
            clients = [PostgresClient(table) for _ in range(args.clients)]
        else:
            import redis
            admin = RedisClient(prefix)
            settings = {key: admin.conn.config_get(key).get(key) for key in ("appendonly", "appendfsync")}
            if settings != {"appendonly": "yes", "appendfsync": "always"}:
                raise RuntimeError(f"Redis AOF with appendfsync=always required: {settings}")
            metadata.update(settings)
            metadata["version"] = admin.conn.info("server")["redis_version"]
            metadata["driver_version"] = redis.__version__
            metadata["transport"] = "TCP loopback through container port forwarding"
            clients = [RedisClient(prefix) for _ in range(args.clients)]

        initial = bytes(args.value_bytes)
        for key in keys:
            clients[0].put(key, initial)
        for key in keys[:100]:
            if clients[0].get(key) != initial:
                raise RuntimeError("warm-up validation failed")
        barrier = threading.Barrier(args.clients + 1)

        def worker(index):
            rng = random.Random(0xDB001 + index)
            client = clients[index]
            reads, writes = [], []
            barrier.wait()
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
            return reads, writes

        with concurrent.futures.ThreadPoolExecutor(max_workers=args.clients) as pool:
            futures = [pool.submit(worker, index) for index in range(args.clients)]
            started = time.perf_counter_ns()
            barrier.wait()
            outputs = [future.result() for future in futures]
            elapsed = time.perf_counter_ns() - started
        for key in keys:
            value = clients[0].get(key)
            if len(value) != args.value_bytes or any(byte != value[0] for byte in value):
                raise RuntimeError("post-run value verification failed")
        reads = [sample for output in outputs for sample in output[0]]
        writes = [sample for output in outputs for sample in output[1]]
        return {"backend": args.backend, "clients": args.clients, "operations_per_client": args.operations,
                "keys": args.keys, "write_percent": args.write_percent, "value_bytes": args.value_bytes,
                "completed": len(reads) + len(writes), "elapsed_ms": round(elapsed / 1e6, 2),
                "ops_per_sec": round((len(reads) + len(writes)) * 1e9 / elapsed, 1),
                "read": latency(reads), "write": latency(writes), "metadata": metadata}
    finally:
        for client in clients:
            client.close()
        if admin is not None:
            try:
                if args.backend == "postgres":
                    admin.conn.execute(f"DROP TABLE IF EXISTS {table}")
                else:
                    admin.conn.delete(*(prefix + key for key in keys))
            finally:
                admin.close()
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
    parser.add_argument("backend", choices=("prototype", "sqlite", "postgres", "redis"))
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
    print(json.dumps(run(args), sort_keys=True))


if __name__ == "__main__":
    main()
