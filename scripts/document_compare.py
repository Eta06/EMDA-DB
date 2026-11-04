#!/usr/bin/env python3
"""Disposable, serial JSON-document comparison; see docs/DOCUMENT_MODEL.md.

Runs the Rust prototype/SQLite in process and native MongoDB/PostgreSQL drivers
through loopback containers. Transport, models and acknowledgements differ.
No images are pulled, no unrelated services are changed, and all failures and
VM/cgroup noise observations are retained in the result file.
"""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import secrets
import subprocess
import sys
import time

from benchmarks.services import LocalService
from benchmarks.telemetry import difference, parse_cpus, quiet_window, read, snapshot

BACKENDS = ("prototype", "sqlite", "postgres", "mongo")
WARMUP = 20


def latency(values):
    values = sorted(values)
    def at(percent):
        return values[math.ceil(len(values) * percent) - 1] / 1000
    return {"count": len(values), "p50_us": at(.5), "p95_us": at(.95),
            "p99_us": at(.99), "max_us": values[-1] / 1000}


def execute_cycles(cycles, create, get, update, lookup, delete):
    samples = {name: [] for name in ("create", "read", "update", "indexed_query", "delete")}
    wall = 0
    for number in range(cycles + WARMUP):
        identifier = f"id-{number}"
        email = f"user-{number}@example.test"
        first = {"email": email, "age": 20, "active": True}
        second = {"email": email, "age": 21, "active": True}
        measured = number >= WARMUP
        cycle_start = time.perf_counter_ns()
        def call(name, function):
            start = time.perf_counter_ns()
            result = function()
            elapsed = time.perf_counter_ns() - start
            if measured:
                samples[name].append(elapsed)
            return result
        call("create", lambda: create(identifier, first))
        if call("read", lambda: get(identifier)) != first:
            raise RuntimeError("read validation failed")
        if call("update", lambda: update(identifier, second)) != 1:
            raise RuntimeError("update validation failed")
        if call("indexed_query", lambda: lookup(email)) != (identifier, second):
            raise RuntimeError("index query validation failed")
        if call("delete", lambda: delete(identifier)) != 1:
            raise RuntimeError("delete validation failed")
        if measured:
            wall += time.perf_counter_ns() - cycle_start
    return {"cycles": cycles, "warmup_cycles": WARMUP,
            "wall_ops_per_sec": cycles * 5 * 1e9 / wall,
            "latency": {name: latency(values) for name, values in samples.items()}}


def postgres(service, cycles):
    import psycopg
    from psycopg.types.json import Jsonb
    args = dict(host="127.0.0.1", port=service.port, dbname="benchmark", user="postgres",
                password=service.env["EMDA_BENCH_PG_PASSWORD"], connect_timeout=5, autocommit=True)
    table = "document_bench_" + secrets.token_hex(8)
    conn = psycopg.connect(**args)
    try:
        settings = {name: conn.execute("SHOW " + name).fetchone()[0]
                    for name in ("fsync", "synchronous_commit", "full_page_writes")}
        if any(value != "on" for value in settings.values()):
            raise RuntimeError("PostgreSQL fsync, synchronous_commit and full_page_writes must be on")
        version = conn.execute("SELECT version()").fetchone()[0]
        conn.execute(f"CREATE TABLE {table} (id TEXT PRIMARY KEY, body JSONB NOT NULL)")
        conn.execute(f"CREATE UNIQUE INDEX ON {table} ((body->>'email'))")
        conn.execute("SET enable_seqscan=off")
        plan = " ".join(row[0] for row in conn.execute(
            f"EXPLAIN SELECT id,body FROM {table} WHERE body->>'email'=%s", ("probe@example.test",)))
        if "Index Scan" not in plan:
            raise RuntimeError("PostgreSQL did not select the email index")
        def create(identifier, body):
            conn.execute(f"INSERT INTO {table}(id,body) VALUES(%s,%s)", (identifier, Jsonb(body)))
        def get(identifier):
            row = conn.execute(f"SELECT body FROM {table} WHERE id=%s", (identifier,)).fetchone()
            return row[0] if row else None
        def update(identifier, body):
            return conn.execute(f"UPDATE {table} SET body=%s WHERE id=%s", (Jsonb(body), identifier)).rowcount
        def lookup(email):
            return conn.execute(f"SELECT id,body FROM {table} WHERE body->>'email'=%s", (email,)).fetchone()
        def delete(identifier):
            return conn.execute(f"DELETE FROM {table} WHERE id=%s", (identifier,)).rowcount
        result = execute_cycles(cycles, create, get, update, lookup, delete)
        conn.close()
        conn = psycopg.connect(**args)
        result["reopen_verified"] = conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0] == 0
        if not result["reopen_verified"]:
            raise RuntimeError("PostgreSQL reopen verification failed")
        result.update(engine="postgres-jsonb", version=version, driver_version=psycopg.__version__,
                      durability=settings, transport="Python psycopg over loopback container port", query_plan=plan,
                      enable_seqscan=False)
        return result
    finally:
        if conn.closed:
            conn = psycopg.connect(**args)
        conn.execute(f"DROP TABLE IF EXISTS {table}")
        conn.close()


def mongo(service, cycles):
    import pymongo
    from pymongo.write_concern import WriteConcern
    name = "document_bench_" + secrets.token_hex(8)
    def connect():
        return pymongo.MongoClient(host="127.0.0.1", port=service.port, serverSelectionTimeoutMS=5000)
    client = connect()
    try:
        collection = client[name].get_collection("docs", write_concern=WriteConcern(w=1, j=True))
        index_name = collection.create_index("email", unique=True)
        def create(identifier, body):
            collection.insert_one({"_id": identifier, **body})
        def get(identifier):
            found = collection.find_one({"_id": identifier})
            if found is not None:
                found.pop("_id")
            return found
        def update(identifier, body):
            return collection.replace_one({"_id": identifier}, {"_id": identifier, **body}).matched_count
        def lookup(email):
            found = collection.find_one({"email": email}, hint=index_name)
            if found is None:
                return None
            identifier = found.pop("_id")
            return identifier, found
        def delete(identifier):
            return collection.delete_one({"_id": identifier}).deleted_count
        result = execute_cycles(cycles, create, get, update, lookup, delete)
        version = client.server_info()["version"]
        client.close()
        client = connect()
        result["reopen_verified"] = client[name].docs.count_documents({}) == 0
        if not result["reopen_verified"]:
            raise RuntimeError("MongoDB reopen verification failed")
        result.update(engine="mongodb", version=version, driver_version=pymongo.version,
                      durability={"w": 1, "j": True}, index_hint=index_name,
                      transport="Python pymongo over loopback container port")
        return result
    finally:
        client.drop_database(name)
        client.close()


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--directory", required=True)
    parser.add_argument("--binary", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--rounds", type=int, default=4)
    parser.add_argument("--cycles", type=int, default=200)
    parser.add_argument("--driver-cpus", default="0")
    parser.add_argument("--server-cpus", default="1-3")
    args = parser.parse_args()
    available = os.sched_getaffinity(0)
    driver, servers = parse_cpus(args.driver_cpus), parse_cpus(args.server_cpus)
    if args.rounds < 2 or not 1 <= args.cycles <= 100000 or not Path(args.directory).is_dir() or not (driver | servers) <= available or driver & servers:
        parser.error("invalid directory, rounds, cycles or CPU sets")
    os.sched_setaffinity(0, driver)
    result = {"revision": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
              "git_status": subprocess.check_output(["git", "status", "--short"], text=True),
              "binary_sha256": sha(args.binary), "script_sha256": sha(__file__),
              "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
              "platform": platform.platform(), "python": sys.version,
              "filesystem": json.loads(subprocess.check_output(["findmnt", "-J", "-T", args.directory, "-o", "TARGET,FSTYPE"], text=True)),
              "controls": {"driver_cpus": sorted(driver), "server_cpus": sorted(servers),
                           "cpu_quota": read("/sys/fs/cgroup/cpu.max"), "one_backend_at_a_time": True,
                           "notes": "No compiler, image pulls, cache drops, host-service changes, or weakened durability during measurement. VM/cgroup telemetry is not per-engine accounting."},
              "scope": "One client repeatedly creates, reads, replaces, queries a unique email index, and deletes a small JSON object. Distinct adapters, transport, features and durability mechanisms are not equivalent production systems.",
              "services": {}, "runs": [], "failures": []}
    services = {}
    def save():
        Path(args.output).write_text(json.dumps(result, indent=2) + "\n")
    try:
        for backend in ("postgres", "mongo"):
            services[backend] = LocalService(backend, args.directory, args.server_cpus)
        for round_index in range(args.rounds):
            order = BACKENDS[round_index % len(BACKENDS):] + BACKENDS[:round_index % len(BACKENDS)]
            for backend in order:
                service = services.get(backend)
                entry = {"backend": backend, "round": round_index}
                try:
                    if service:
                        service.start()
                        result["services"][backend] = service.metadata()
                    entry["pre_run_quiet"] = quiet_window(seconds=.25, attempts=3)
                    before = snapshot()
                    try:
                        if backend in ("prototype", "sqlite"):
                            command = [args.binary, backend, args.directory, str(args.cycles)]
                            completed = subprocess.run(command, capture_output=True, text=True, timeout=180, check=True)
                            entry["measurement"] = json.loads(completed.stdout)
                            entry["command"] = command
                        else:
                            entry["measurement"] = (postgres if backend == "postgres" else mongo)(service, args.cycles)
                    finally:
                        entry["noise"] = difference(before, snapshot())
                except Exception as error:
                    entry["error"] = f"{type(error).__name__}: {error}"
                    result["failures"].append({"backend": backend, "round": round_index, "error": entry["error"]})
                finally:
                    result["runs"].append(entry)
                    save()
                    if service:
                        try:
                            service.stop()
                        except Exception as error:
                            entry["cleanup_error"] = f"{type(error).__name__}: {error}"
                            result["failures"].append({"backend": backend, "round": round_index,
                                                       "error": "service stop: " + entry["cleanup_error"]})
                            save()
    finally:
        for backend, service in services.items():
            try:
                service.close()
            except Exception as error:
                result["failures"].append({"backend": backend, "error": f"service cleanup: {type(error).__name__}: {error}"})
        save()
    if result["failures"]:
        raise SystemExit("one or more document comparison runs failed; see output")


if __name__ == "__main__":
    main()
