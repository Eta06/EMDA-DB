#!/usr/bin/env python3
"""Compare two-key serializable transactions with SQLite on a Linux VM."""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import statistics
import subprocess
import time

from benchmarks.telemetry import difference, quiet_window, read, snapshot

MODES = ("conservative", "readset", "sqlite")


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def percentile(values, fraction):
    ordered = sorted(values)
    return ordered[math.ceil(len(ordered) * fraction) - 1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--binary", default="target/release/examples/transaction_bench")
    parser.add_argument("--clients", default="1,4,8")
    parser.add_argument("--operations", type=int, default=100)
    parser.add_argument("--rounds", type=int, default=5)
    args = parser.parse_args()
    try:
        clients = [int(value) for value in args.clients.split(",")]
    except ValueError:
        parser.error("clients must be comma-separated integers")
    if (not clients or len(set(clients)) != len(clients) or any(not 1 <= n <= 8 for n in clients)
            or not 1 <= args.operations <= 10_000 or not 2 <= args.rounds <= 50):
        parser.error("invalid clients, operation count or rounds")
    if not Path(args.directory).is_dir() or not Path(args.binary).is_file() or os.path.lexists(args.output):
        parser.error("existing fixture directory and binary, and a new output path, are required")
    allowed = sorted(os.sched_getaffinity(0))
    if len(allowed) < 2:
        parser.error("at least two allowed CPUs are required")
    benchmark_cpus, driver_cpu = allowed[:min(4, len(allowed) - 1)], allowed[-1]
    os.sched_setaffinity(0, {driver_cpu})
    source_paths = sorted([*Path("src").rglob("*.rs"), Path("Cargo.toml"), Path("Cargo.lock"),
                           Path("examples/transaction_bench.rs"), Path("scripts/benchmarks/telemetry.py"), Path(__file__)])
    report = {
        "revision": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "git_status": subprocess.check_output(["git", "status", "--short"], text=True),
        "source_sha256": {str(path): sha256(path) for path in source_paths}, "binary_sha256": sha256(args.binary),
        "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "cpu_model": next((line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines() if line.startswith("model name")), "unknown"),
        "cpu_quota": read("/sys/fs/cgroup/cpu.max"), "allowed_cpus": allowed,
        "benchmark_cpus": benchmark_cpus, "driver_cpu": driver_cpu,
        "filesystem": json.loads(subprocess.check_output(["findmnt", "-J", "-T", args.directory, "-o", "TARGET,FSTYPE"], text=True)),
        "limitations": "One VM, synthetic warm-cache direct APIs, bounded closed-loop load, fresh process and fixture per run. Each operation reads and updates two checksummed values atomically; SQLite uses WAL, synchronous=FULL and BEGIN IMMEDIATE. Prototype modes use synchronized optimistic transactions and explicitly retry conflicts. SQLite's internal busy waiting is reflected in latency, not conflict counts. Implementations, storage and durability behavior differ; no physical power-loss or general competitor ranking is established. VM/cgroup noise includes setup, verification and cleanup.",
        "cases": [],
    }
    cpus = ",".join(map(str, benchmark_cpus))
    for workload in ("disjoint", "shared"):
        for count in clients:
            case = {"workload": workload, "clients": count, "operations_per_client": args.operations,
                    "quiet_check": quiet_window(), "runs": []}
            for round_index in range(args.rounds):
                order = MODES[round_index % len(MODES):] + MODES[:round_index % len(MODES)]
                for mode in order:
                    before = snapshot()
                    command = ["taskset", "-c", cpus, args.binary, mode, str(count), str(args.operations), workload, args.directory]
                    process = subprocess.run(command, capture_output=True, text=True, timeout=300, check=True)
                    noise = difference(before, snapshot())
                    values = dict(part.split("=", 1) for part in process.stdout.strip().split())
                    if (values["backend"] != mode or int(values["clients"]) != count
                            or int(values["operations"]) != args.operations or values["workload"] != workload):
                        raise RuntimeError("unexpected transaction benchmark configuration")
                    samples = [int(value) for value in values["samples_ns"].split(",")]
                    conflicts = [int(value) for value in values["conflicts"].split(",")]
                    if len(samples) != count * args.operations or len(conflicts) != count:
                        raise RuntimeError("incomplete transaction benchmark samples")
                    elapsed = int(values["elapsed_ns"])
                    case["runs"].append({"round": round_index + 1, "mode": mode, "version": values["version"],
                        "elapsed_ns": elapsed, "operations_per_second": len(samples) * 1e9 / elapsed,
                        "conflicts_per_client": conflicts, "samples_ns": samples,
                        "p50_ns": percentile(samples, .5), "p95_ns": percentile(samples, .95),
                        "p99_ns": percentile(samples, .99), "max_ns": max(samples), "noise": noise})
            case["summary"] = {}
            for mode in MODES:
                runs = [run for run in case["runs"] if run["mode"] == mode]
                case["summary"][mode] = {
                    "median_ops_per_second": statistics.median(run["operations_per_second"] for run in runs),
                    "median_p50_ns": statistics.median(run["p50_ns"] for run in runs),
                    "median_p99_ns": statistics.median(run["p99_ns"] for run in runs),
                    "median_conflicts": statistics.median(sum(run["conflicts_per_client"]) for run in runs),
                    "min_ops_per_second": min(run["operations_per_second"] for run in runs),
                    "max_ops_per_second": max(run["operations_per_second"] for run in runs),
                }
            report["cases"].append(case)
    with open(args.output, "x", encoding="utf-8") as output:
        json.dump(report, output, indent=2)
        output.write("\n")
    for case in report["cases"]:
        print(f"workload={case['workload']} clients={case['clients']} summary={case['summary']}")


if __name__ == "__main__":
    main()
