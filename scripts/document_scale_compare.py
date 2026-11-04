#!/usr/bin/env python3
"""Compare embedded document operations with a nonempty indexed collection."""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import subprocess
import sys
import time

from benchmarks.telemetry import difference, quiet_window, read, snapshot

OPERATIONS = ("create", "read", "update", "indexed_query", "delete")


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def validate_result(result, backend, size, cycles):
    expected = "sqlite-json" if backend == "sqlite" else "document-prototype"
    if backend not in ("prototype", "prototype-index", "prototype-planned", "prototype-scan", "sqlite"):
        raise ValueError("unknown benchmark backend")
    if (result.get("engine") != expected or result.get("preloaded_documents") != size
            or result.get("cycles") != cycles or result.get("reopen_verified") is not True):
        raise ValueError("benchmark identity, size, cycle count or reopen verification mismatch")
    rate = result.get("wall_ops_per_sec")
    if type(rate) not in (int, float) or not math.isfinite(rate) or rate <= 0:
        raise ValueError("invalid benchmark throughput")
    expected_mode = {"prototype-index": "index", "prototype-planned": "planned", "prototype-scan": "scan"}.get(backend)
    if expected_mode and result.get("query_mode") != expected_mode:
        raise ValueError("prototype used the wrong query mode")
    if backend != "sqlite" and result.get("log_format") not in ("V2", "V3"):
        raise ValueError("missing or unsupported prototype log format")
    if backend == "sqlite" and "USING INDEX" not in result.get("query_plan", ""):
        raise ValueError("SQLite did not use the expected index")
    for operation in OPERATIONS:
        samples = result.get("latency", {}).get(operation, {})
        if samples.get("count") != cycles or len(samples.get("samples_ns", [])) != cycles:
            raise ValueError(f"missing or incomplete {operation} samples")
        if any(type(value) is not int or value < 0 for value in samples["samples_ns"]):
            raise ValueError(f"invalid {operation} sample")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", required=True)
    parser.add_argument("--binary", required=True)
    parser.add_argument("--baseline-binary")
    parser.add_argument("--compare-plans", action="store_true", help="compare complete scans, planned unique-index queries, and SQLite")
    parser.add_argument("--output", required=True)
    parser.add_argument("--sizes", default="0,100,1000,3000")
    parser.add_argument("--rounds", type=int, default=4)
    parser.add_argument("--cycles", type=int, default=100)
    args = parser.parse_args()
    try:
        sizes = [int(value) for value in args.sizes.split(",")]
    except ValueError:
        parser.error("sizes must be comma-separated integers")
    allowed = sorted(os.sched_getaffinity(0))
    if (not Path(args.directory).is_dir() or not Path(args.binary).is_file()
            or (args.baseline_binary is not None and not Path(args.baseline_binary).is_file())
            or (args.compare_plans and args.baseline_binary is not None)
            or os.path.lexists(args.output) or len(allowed) < 2 or not sizes
            or len(set(sizes)) != len(sizes) or any(not 0 <= size <= 5000 for size in sizes)
            or not 2 <= args.rounds <= 50 or not 1 <= args.cycles <= 100000):
        parser.error("invalid paths, CPU availability, sizes, rounds or cycle count")
    benchmark_cpu, driver_cpu = allowed[0], allowed[-1]
    os.sched_setaffinity(0, {driver_cpu})
    report = {
        "revision": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "git_status": subprocess.check_output(["git", "status", "--short"], text=True),
        "binary_sha256": sha256(args.binary), "script_sha256": sha256(__file__),
        "baseline_binary_sha256": sha256(args.baseline_binary) if args.baseline_binary else None,
        "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "platform": platform.platform(), "python": sys.version,
        "cpu_model": next((line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines() if line.startswith("model name")), "unknown"),
        "cpu_quota": read("/sys/fs/cgroup/cpu.max"), "benchmark_cpu": benchmark_cpu, "driver_cpu": driver_cpu,
        "filesystem": json.loads(subprocess.check_output(["findmnt", "-J", "-T", args.directory, "-o", "TARGET,FSTYPE"], text=True)),
        "scope": "Single client; Rust in-process prototype and SQLite; small JSON object with a unique email index; preload in 32-document transactions, then create/read/replace/query/delete transient documents. Preparation and verification are excluded from operation timers but included in VM/cgroup telemetry. RSS is process high-water mark, excludes page cache and is not a project memory limit. SQLite returns JSON text while the prototype decodes JSON. No concurrency, physical power loss, off-host restore or universal ranking is measured.",
        "sizes": sizes, "rounds": args.rounds, "cycles": args.cycles, "runs": [], "failures": [],
    }
    if args.compare_plans:
        report["scope"] = "Single client; prototype complete scan, rule-based unique-equality plan, explicit index lookup, and SQLite expression-index lookup. SQLite returns JSON text; the prototype decodes and validates JSON. Scan runs are omitted when the collection exceeds the 1024-document limit. Preparation and recovery verification are outside operation timers but included in telemetry. Not a concurrency, power-loss or universal ranking."
    output = Path(args.output)
    with output.open("x", encoding="utf-8") as file:
        json.dump(report, file, indent=2)
        file.write("\n")

    def save():
        output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")

    targets = [("prototype", "prototype", args.binary), ("sqlite", "sqlite", args.binary)]
    if args.baseline_binary:
        targets = [("before", "prototype", args.baseline_binary),
                   ("after", "prototype", args.binary), ("sqlite", "sqlite", args.binary)]
    for size_index, size in enumerate(sizes):
        if args.compare_plans:
            targets = [("planned", "prototype-planned", args.binary), ("explicit", "prototype-index", args.binary), ("sqlite", "sqlite", args.binary)]
            if size < 1024:
                targets.insert(0, ("scan", "prototype-scan", args.binary))
        for round_index in range(args.rounds):
            rotation = (size_index + round_index) % len(targets)
            order = targets[rotation:] + targets[:rotation]
            for variant, backend, binary in order:
                entry = {"variant": variant, "backend": backend, "preloaded_documents": size, "round": round_index + 1}
                try:
                    entry["pre_run_quiet"] = quiet_window(seconds=.25, attempts=3)
                    before = snapshot()
                    try:
                        command = ["taskset", "-c", str(benchmark_cpu), binary, backend,
                                   args.directory, str(args.cycles), str(size)]
                        completed = subprocess.run(command, capture_output=True, text=True, timeout=300)
                        if completed.returncode:
                            raise RuntimeError(f"benchmark exited {completed.returncode}: {completed.stderr[-4096:]}")
                        result = json.loads(completed.stdout)
                        validate_result(result, backend, size, args.cycles)
                        entry["measurement"] = result
                        entry["command"] = command
                    finally:
                        entry["noise"] = difference(before, snapshot())
                except Exception as error:
                    entry["error"] = f"{type(error).__name__}: {error}"
                    report["failures"].append({"variant": variant, "backend": backend, "preloaded_documents": size,
                                               "round": round_index + 1, "error": entry["error"]})
                finally:
                    report["runs"].append(entry)
                    save()
    if report["failures"]:
        raise SystemExit("one or more document scale runs failed; see output")
    print(f"Completed {len(report['runs'])} document scale runs")


if __name__ == "__main__":
    main()
