#!/usr/bin/env python3
"""Compare compound range reads with disposable PostgreSQL and MongoDB servers.

Rust prototype and SQLite share one process; PostgreSQL/MongoDB use Python
clients over loopback. Their APIs, validation, durability and overhead differ.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import secrets
import subprocess
import sys
import time

from benchmarks.services import LocalService
from benchmarks.telemetry import difference, parse_cpus, quiet_window, read, snapshot
from nonunique_query_compare import parse_cases, validate

WARMUP = 10
BACKENDS = ("local", "postgres", "mongo")


def documents(total, matches):
    return [(f"id-{number:05}", {
        "tenant": "target" if number < matches or number % 3 == 0 else "other",
        "number": total - 1 - number,
        "payload": "small document value",
    }) for number in range(total)]


def uses_index(value, index_name):
    if isinstance(value, dict):
        postgres_index = value.get("Index Name") == index_name and value.get("Node Type") in (
            "Index Scan", "Index Only Scan", "Bitmap Index Scan")
        mongo_index = value.get("indexName") == index_name and value.get("stage") in (
            "IXSCAN", "DISTINCT_SCAN", "EXPRESS_IXSCAN")
        if postgres_index or mongo_index:
            return True
        return any(uses_index(child, index_name) for child in value.values())
    if isinstance(value, list):
        return any(uses_index(child, index_name) for child in value)
    return False


def measure(cycles, fetch, expected):
    values = []
    for cycle in range(WARMUP + cycles):
        start = time.perf_counter_ns()
        actual = fetch()
        elapsed = time.perf_counter_ns() - start
        if actual != expected:
            raise RuntimeError("query results differ from the expected IDs and documents")
        if cycle >= WARMUP:
            values.append(elapsed)
    return values


def postgres(service, total, matches, cycles):
    import psycopg
    from psycopg.types.json import Jsonb

    args = {"host": "127.0.0.1", "port": service.port, "dbname": "benchmark", "user": "postgres",
            "password": service.env["EMDA_BENCH_PG_PASSWORD"], "connect_timeout": 5, "autocommit": True}
    table = "compound_range_" + secrets.token_hex(8)
    index = table + "_idx"
    data = documents(total, matches)
    expected = data[:matches]
    threshold = total - matches
    conn = psycopg.connect(**args)
    try:
        settings = {key: conn.execute("SHOW " + key).fetchone()[0]
                    for key in ("fsync", "synchronous_commit", "full_page_writes")}
        if any(value != "on" for value in settings.values()):
            raise RuntimeError("PostgreSQL durability settings are not enabled")
        version = conn.execute("SELECT version()").fetchone()[0]
        conn.execute(f"CREATE TABLE {table} (id TEXT PRIMARY KEY, body JSONB NOT NULL)")
        conn.execute(f"CREATE INDEX {index} ON {table} ((body->>'tenant'), (((body->>'number')::bigint)))")
        for start in range(0, total, 32):
            with conn.transaction():
                with conn.cursor() as cursor:
                    cursor.executemany(f"INSERT INTO {table}(id,body) VALUES(%s,%s)",
                                       [(identifier, Jsonb(body)) for identifier, body in data[start:start+32]])
        conn.execute(f"ANALYZE {table}")
        conn.execute("SET enable_seqscan=off")
        conn.execute("SET enable_indexscan=off")
        conn.execute("SET enable_indexonlyscan=off")
        sql = f"SELECT id,body FROM {table} WHERE body->>'tenant'=%s AND (body->>'number')::bigint >= %s ORDER BY id"
        plan = conn.execute("EXPLAIN (FORMAT JSON) " + sql, ("target", threshold)).fetchone()[0]
        if not uses_index(plan, index):
            raise RuntimeError("PostgreSQL did not use the requested compound index")
        values = measure(cycles, lambda: conn.execute(sql, ("target", threshold)).fetchall(), expected)
        conn.close()
        conn = psycopg.connect(**args)
        if conn.execute(f"SELECT id,body FROM {table} ORDER BY id").fetchall() != data:
            raise RuntimeError("PostgreSQL reopened content differs")
        return {"engine": "postgres-jsonb", "version": version, "driver_version": psycopg.__version__,
                "durability": settings, "transport": "Python psycopg over loopback container port",
                "planner_settings": {"enable_seqscan": "off", "enable_indexscan": "off", "enable_indexonlyscan": "off"}, "query_plan": plan,
                "samples_ns": values, "reopen_verified": True}
    finally:
        if conn.closed:
            conn = psycopg.connect(**args)
        try:
            conn.execute(f"DROP TABLE IF EXISTS {table}")
        finally:
            conn.close()


def mongo(service, total, matches, cycles):
    import pymongo
    from bson.json_util import dumps
    from pymongo.write_concern import WriteConcern

    name = "compound_range_" + secrets.token_hex(8)
    data = documents(total, matches)
    expected = data[:matches]
    threshold = total - matches
    def connect():
        return pymongo.MongoClient(host="127.0.0.1", port=service.port, serverSelectionTimeoutMS=5000)
    client = connect()
    try:
        collection = client[name].get_collection("docs", write_concern=WriteConcern(w=1, j=True))
        index = collection.create_index([("tenant", 1), ("number", 1)], name="tenant_number")
        for start in range(0, total, 32):
            collection.insert_many([{"_id": identifier, **body} for identifier, body in data[start:start+32]])
        condition = {"tenant": "target", "number": {"$gte": threshold}}
        def fetch():
            rows = list(collection.find(condition).hint(index).sort("_id", 1))
            return [(row.pop("_id"), row) for row in rows]
        plan = json.loads(dumps(collection.find(condition).hint(index).sort("_id", 1).explain()))
        if not uses_index(plan.get("queryPlanner", {}).get("winningPlan"), index):
            raise RuntimeError("MongoDB did not use the hinted compound index")
        values = measure(cycles, fetch, expected)
        version = client.server_info()["version"]
        client.close()
        client = connect()
        restored = [(row.pop("_id"), row) for row in client[name].docs.find().sort("_id", 1)]
        if restored != data:
            raise RuntimeError("MongoDB reopened content differs")
        return {"engine": "mongodb", "version": version, "driver_version": pymongo.version,
                "durability": {"w": 1, "j": True}, "transport": "Python pymongo over loopback container port",
                "index_hint": index, "query_plan": plan, "samples_ns": values, "reopen_verified": True}
    finally:
        try:
            client.drop_database(name)
        finally:
            client.close()


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", required=True)
    parser.add_argument("--binary", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--cases", default="100:10,1000:10,1000:100")
    parser.add_argument("--rounds", type=int, default=4)
    parser.add_argument("--cycles", type=int, default=50)
    parser.add_argument("--driver-cpus", default="0")
    parser.add_argument("--server-cpus", default="1-3")
    args = parser.parse_args()
    try:
        cases = parse_cases(args.cases)
        driver, servers = parse_cpus(args.driver_cpus), parse_cpus(args.server_cpus)
    except ValueError as error:
        parser.error(str(error))
    available = os.sched_getaffinity(0)
    if (not Path(args.directory).is_dir() or not Path(args.binary).is_file()
            or os.path.lexists(args.output) or not (driver | servers) <= available
            or driver & servers or not 2 <= args.rounds <= 50 or not 1 <= args.cycles <= 100000):
        parser.error("invalid paths, CPU sets, rounds or cycles")
    os.sched_setaffinity(0, driver)
    result = {
        "revision": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "git_status": subprocess.check_output(["git", "status", "--short"], text=True),
        "binary_sha256": sha(args.binary), "script_sha256": sha(__file__),
        "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "platform": platform.platform(), "python": sys.version,
        "cpu_model": next((line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines() if line.startswith("model name")), "unknown"),
        "filesystem": json.loads(subprocess.check_output(["findmnt", "-J", "-T", args.directory, "-o", "TARGET,FSTYPE"], text=True)),
        "controls": {"driver_cpus": sorted(driver), "server_cpus": sorted(servers), "cpu_quota": read("/sys/fs/cgroup/cpu.max")},
        "scope": "One client; equality-prefix and scalar-range reads returning the same IDs and decoded documents. Prototype/SQLite share an in-process Rust adapter and interleave four paths; PostgreSQL/MongoDB use Python drivers over loopback to disposable containers. Indexed plans are forced or hinted. Different models, validation, transport and durability do not establish equivalent engines or a universal ranking. Setup and reopened-content checks are outside operation timers but inside VM telemetry.",
        "cases": cases, "rounds": args.rounds, "cycles": args.cycles, "services": {}, "runs": [], "failures": [],
    }
    output = Path(args.output)
    with output.open("x", encoding="utf-8") as file:
        json.dump(result, file, indent=2)
        file.write("\n")
    def save():
        output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    services = {}
    try:
        for backend in ("postgres", "mongo"):
            try:
                services[backend] = LocalService(backend, args.directory, args.server_cpus)
            except Exception as error:
                result["failures"].append({"backend": backend, "error": f"service creation failed: {type(error).__name__}: {error}"})
                raise
        for round_index in range(args.rounds):
            rotation = round_index % len(cases)
            for total, matches in cases[rotation:] + cases[:rotation]:
                order = BACKENDS[round_index % len(BACKENDS):] + BACKENDS[:round_index % len(BACKENDS)]
                for backend in order:
                    entry = {"backend": backend, "round": round_index + 1, "documents": total, "matches": matches}
                    service = services.get(backend)
                    started = False
                    try:
                        if service:
                            service.start()
                            started = True
                            result["services"][backend] = service.metadata()
                        entry["pre_run_quiet"] = quiet_window(seconds=.25, attempts=3)
                        before = snapshot()
                        try:
                            if backend == "local":
                                command = [args.binary, args.directory, str(total), str(matches), str(args.cycles)]
                                completed = subprocess.run(command, capture_output=True, text=True, timeout=300, check=True)
                                measurement = json.loads(completed.stdout)
                                validate(measurement, total, matches, args.cycles, "docs_tenant_number")
                                entry["command"] = command
                            else:
                                measurement = (postgres if backend == "postgres" else mongo)(service, total, matches, args.cycles)
                            entry["measurement"] = measurement
                        finally:
                            entry["noise"] = difference(before, snapshot())
                    except Exception as error:
                        message = f"{type(error).__name__}: {error}"
                        if service and service.env.get("EMDA_BENCH_PG_PASSWORD"):
                            message = message.replace(service.env["EMDA_BENCH_PG_PASSWORD"], "<redacted>")
                        entry["error"] = message
                        result["failures"].append({"backend": backend, "round": round_index + 1, "documents": total, "matches": matches, "error": message})
                    finally:
                        if started:
                            try:
                                service.stop()
                            except Exception as error:
                                entry["cleanup_error"] = f"{type(error).__name__}: {error}"
                                result["failures"].append({"backend": backend, "round": round_index + 1, "error": "service stop failed"})
                        result["runs"].append(entry)
                        save()
    finally:
        for backend, service in services.items():
            try:
                service.close()
            except Exception as error:
                result["failures"].append({"backend": backend, "error": f"service cleanup failed: {type(error).__name__}: {error}"})
        result["ended_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        save()
    if result["failures"]:
        raise SystemExit("one or more comparison runs failed; see output")
    print(f"Completed {len(result['runs'])} verified comparison runs")


if __name__ == "__main__":
    main()
