#!/usr/bin/env python3
"""Repeat the in-process concurrent document benchmark on a Linux fixture filesystem."""
import argparse
import gzip
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import resource
import subprocess
import tempfile

from benchmarks import document_contention
from benchmarks.services import LocalService
from benchmarks.telemetry import difference, parse_cpus, quiet_window, snapshot

ENGINES = ("store", "checkpoint", "sqlite")
SERVERS = {"postgres": document_contention.postgres, "mongo": document_contention.mongo}


def validate(result, engine, clients, cycles, documents):
    if (result.get("engine") != engine or result.get("clients") != clients
            or result.get("cycles_per_client") != cycles or result.get("documents") != documents
            or result.get("reopen_verified") is not True):
        raise ValueError("benchmark identity or reopen verification mismatch")
    throughput = result.get("ops_per_sec")
    if type(throughput) not in (int, float) or not math.isfinite(throughput) or throughput <= 0:
        raise ValueError("invalid benchmark throughput")
    samples = result.get("clients_samples", [])
    if len(samples) != clients:
        raise ValueError("incorrect client sample count")
    for client in samples:
        for name in ("update_ns", "query_ns"):
            values = client.get(name, [])
            if len(values) != cycles or any(type(value) is not int or value < 0 for value in values):
                raise ValueError(f"invalid samples for {name}")
        for name in ("update_retries", "query_retries"):
            if type(client.get(name)) is not int or client[name] < 0:
                raise ValueError(f"invalid count for {name}")


def run_embedded(run, binary, engine, directory, clients, cycles, documents):
    command = [str(binary.resolve()), engine, str(directory.resolve()),
               str(clients), str(cycles), str(documents)]
    run["command"] = command
    before = resource.getrusage(resource.RUSAGE_CHILDREN)
    try:
        completed = subprocess.run(command, text=True, capture_output=True, timeout=360, check=False)
    except subprocess.TimeoutExpired as error:
        run["stdout"] = (error.stdout or b"").decode(errors="replace")
        run["stderr"] = (error.stderr or b"").decode(errors="replace")
        raise
    after = resource.getrusage(resource.RUSAGE_CHILDREN)
    run.update(returncode=completed.returncode, stderr=completed.stderr,
               child_cpu_seconds=after.ru_utime + after.ru_stime - before.ru_utime - before.ru_stime)
    if completed.returncode != 0:
        run["stdout"] = completed.stdout
        raise ValueError(f"{engine} with {clients} clients exited {completed.returncode}")
    try:
        return json.loads(completed.stdout)
    except ValueError:
        run["stdout"] = completed.stdout
        raise


def run_server(run, engine, args, clients):
    service = LocalService(engine, args.directory.resolve(), args.server_cpus)
    result = dict(engine=engine, clients=clients, cycles_per_client=args.cycles,
                  documents=args.documents, reopen_verified=False)
    run["result"] = result
    try:
        service.start()
        run["service"] = service.metadata()
        run["pre_run_quiet"] = quiet_window(seconds=.25, attempts=3)
        SERVERS[engine](service, args.documents, clients, args.cycles, result)
        result["reopen_verified"] = True
        run["returncode"] = 0
        return result
    except Exception as error:
        run["server_error"] = service.redact(str(error))
        raise ValueError(run["server_error"]) from None
    finally:
        try:
            service.close()
        except Exception as error:
            run["cleanup_error"] = service.redact(str(error))
            if "server_error" not in run:
                raise ValueError(run["cleanup_error"]) from None


def save_report(path, report):
    # Keep the most recent complete case report if a later process is killed.
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as handle:
        temporary = Path(handle.name)
    try:
        with gzip.open(temporary, "wt", encoding="utf-8") as output:
            json.dump(report, output)
            output.write("\n")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--directory", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--source-revision", required=True)
    parser.add_argument("--baseline-binary", type=Path)
    parser.add_argument("--baseline-revision")
    parser.add_argument("--clients", default="1,2,4")
    parser.add_argument("--rounds", type=int, default=4)
    parser.add_argument("--cycles", type=int, default=100)
    parser.add_argument("--documents", type=int, default=500)
    parser.add_argument("--driver-cpus", help="optional Linux CPU set for the driver and embedded engines")
    parser.add_argument("--server-cpus", help="enable disposable PostgreSQL and MongoDB on this separate CPU set")
    args = parser.parse_args()
    try:
        clients = [int(value) for value in args.clients.split(",")]
        available = os.sched_getaffinity(0)
        driver = parse_cpus(args.driver_cpus) if args.driver_cpus else available
        servers = parse_cpus(args.server_cpus) if args.server_cpus else set()
    except ValueError:
        parser.error("clients must be comma-separated integers")
    if (not args.binary.is_file() or not args.directory.is_dir() or os.path.lexists(args.output)
            or not (driver | servers) <= available or driver & servers
            or len(set(clients)) != len(clients)
            or not all(1 <= count <= 8 for count in clients)
            or not 2 <= args.rounds <= 20 or not 1 <= args.cycles <= 1000
            or not max(clients) <= args.documents <= 1000
            or bool(args.baseline_binary) != bool(args.baseline_revision)
            or (args.baseline_binary is not None and not args.baseline_binary.is_file())):
        parser.error("invalid benchmark path or workload dimensions")
    root = Path(__file__).resolve().parents[1]
    hashed = {
        "binary": args.binary,
        "driver": Path(__file__),
        "example": root / "examples/document_contention_bench.rs",
    }
    variants = [(engine, engine, args.binary) for engine in ENGINES]
    if servers:
        hashed["server_adapter"] = root / "scripts/benchmarks/document_contention.py"
        hashed["service_harness"] = root / "scripts/benchmarks/services.py"
        variants.extend((engine, engine, None) for engine in SERVERS)
    if args.baseline_binary is not None:
        hashed["baseline_binary"] = args.baseline_binary
        variants.extend(
            (f"{engine}-before", engine, args.baseline_binary)
            for engine in ENGINES if engine != "sqlite"
        )
    fingerprints = {name: hashlib.sha256(path.read_bytes()).hexdigest() for name, path in hashed.items()}
    if fingerprints.get("baseline_binary") == fingerprints["binary"]:
        parser.error("baseline and candidate binaries are identical")
    if args.driver_cpus:
        os.sched_setaffinity(0, driver)
    report = {
        "source_revision": args.source_revision,
        "baseline_revision": args.baseline_revision,
        "sha256": fingerprints,
        "platform": platform.platform(),
        "cpu_affinity": sorted(os.sched_getaffinity(0)),
        "server_cpu_affinity": sorted(servers),
        "filesystem": subprocess.check_output(
            ["findmnt", "-T", args.directory, "-n", "-o", "TARGET,FSTYPE"], text=True
        ).strip(),
        "clients": clients,
        "rounds": args.rounds,
        "cycles_per_client": args.cycles,
        "documents": args.documents,
        "runs": [],
    }
    with args.output.open("xb"):
        pass
    try:
        save_report(args.output, report)
        for dimension, count in enumerate(clients):
            for round_index in range(args.rounds):
                offset = (dimension + round_index) % len(variants)
                order = variants[offset:] + variants[:offset]
                for label, engine, binary in order:
                    run = {"variant": label, "round": round_index + 1, "clients": count,
                           "status": "running"}
                    report["runs"].append(run)
                    save_report(args.output, report)
                    if servers and binary is not None:
                        run["pre_run_quiet"] = quiet_window(seconds=.25, attempts=3)
                    before = snapshot()
                    try:
                        if binary is None:
                            result = run_server(run, engine, args, count)
                        else:
                            result = run_embedded(run, binary, engine, args.directory,
                                                  count, args.cycles, args.documents)
                        run["result"] = result
                        validate(result, engine, count, args.cycles, args.documents)
                        run["status"] = "verified"
                    except Exception as error:
                        run["status"] = "failed"
                        run["error"] = str(error)
                        raise
                    finally:
                        run["telemetry"] = difference(before, snapshot())
                        save_report(args.output, report)
    except BaseException as error:
        report["failure"] = str(error)
        raise
    finally:
        save_report(args.output, report)


if __name__ == "__main__":
    main()
