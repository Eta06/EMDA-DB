#!/usr/bin/env python3
"""Measure one-page range transactions on the checkpoint store and SQLite."""
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

MODES = ("checkpoint", "sqlite")


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def percentile(values, fraction):
    ordered = sorted(values)
    return ordered[math.ceil(len(ordered) * fraction) - 1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--binary", default="target/release/examples/checkpoint_range_bench")
    parser.add_argument("--baseline-binary")
    parser.add_argument("--baseline-revision")
    parser.add_argument("--widths", default="10,1000,10000")
    parser.add_argument("--total", type=int, default=20000)
    parser.add_argument("--operations", type=int, default=100)
    parser.add_argument("--rounds", type=int, default=5)
    args = parser.parse_args()
    try:
        widths = [int(value) for value in args.widths.split(",")]
    except ValueError:
        parser.error("widths must be comma-separated integers")
    if (not 1 <= args.total <= 60000 or not widths or len(set(widths)) != len(widths)
            or any(not 1 <= width <= args.total for width in widths)
            or not 1 <= args.operations <= 10000 or not 2 <= args.rounds <= 50):
        parser.error("invalid total, widths, operation count or rounds")
    if not Path(args.directory).is_dir() or not Path(args.binary).is_file() or os.path.lexists(args.output):
        parser.error("existing fixture directory and binary, and a new output path, are required")
    if bool(args.baseline_binary) != bool(args.baseline_revision):
        parser.error("baseline binary and revision must be supplied together")
    if args.baseline_binary and not Path(args.baseline_binary).is_file():
        parser.error("baseline binary must exist")
    modes = ("baseline", *MODES) if args.baseline_binary else MODES
    allowed = sorted(os.sched_getaffinity(0))
    if len(allowed) < 2:
        parser.error("at least two allowed CPUs are required")
    benchmark_cpu, driver_cpu = allowed[0], allowed[-1]
    os.sched_setaffinity(0, {driver_cpu})
    source_paths = sorted([*Path("src").rglob("*.rs"), Path("Cargo.toml"), Path("Cargo.lock"),
                           Path("examples/checkpoint_range_bench.rs"), Path("scripts/benchmarks/telemetry.py"), Path(__file__)])
    report = {
        "revision": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "git_status": subprocess.check_output(["git", "status", "--short"], text=True),
        "source_sha256": {str(path): sha256(path) for path in source_paths},
        "binary_sha256": sha256(args.binary),
        "baseline_revision": args.baseline_revision,
        "baseline_binary_sha256": sha256(args.baseline_binary) if args.baseline_binary else None,
        "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "cpu_model": next((line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines() if line.startswith("model name")), "unknown"),
        "cpu_quota": read("/sys/fs/cgroup/cpu.max"), "allowed_cpus": allowed,
        "benchmark_cpu": benchmark_cpu, "driver_cpu": driver_cpu,
        "filesystem": json.loads(subprocess.check_output(["findmnt", "-J", "-T", args.directory, "-o", "TARGET,FSTYPE"], text=True)),
        "limitations": "One VM, one client, warm-cache direct APIs and fresh fixtures. Each transaction returns the first item from a range and increments a separate counter with synchronized commit. Checkpoint read-set validation captures every key/location in the requested range; SQLite uses WAL, synchronous=FULL, BEGIN IMMEDIATE and application-level value checksums. Initialization, checkpoint creation and recovery verification occur outside operation timing. A baseline revision, if supplied, is caller-declared and its executable is identified by hash. No concurrent writer, power-loss test, overall memory bound or general competitor ranking is established. VM/cgroup noise includes setup, verification and cleanup.",
        "cases": [],
    }
    for width in widths:
        case = {"width": width, "total_keys": args.total, "operations": args.operations,
                "quiet_check": quiet_window(), "runs": []}
        for index in range(args.rounds):
            order = modes[index % len(modes):] + modes[:index % len(modes)]
            for mode in order:
                before = snapshot()
                executable = args.baseline_binary if mode == "baseline" else args.binary
                backend = "checkpoint" if mode == "baseline" else mode
                command = ["taskset", "-c", str(benchmark_cpu), executable, backend, str(args.total),
                           str(width), str(args.operations), args.directory]
                process = subprocess.run(command, capture_output=True, text=True, timeout=300)
                noise = difference(before, snapshot())
                if process.returncode:
                    raise RuntimeError(f"{mode} benchmark failed ({process.returncode}): {process.stderr.strip()}")
                values = dict(part.split("=", 1) for part in process.stdout.strip().split())
                if (values["backend"] != backend or int(values["total"]) != args.total
                        or int(values["width"]) != width or int(values["operations"]) != args.operations):
                    raise RuntimeError("unexpected benchmark configuration")
                samples = [int(value) for value in values["samples_ns"].split(",")]
                if len(samples) != args.operations:
                    raise RuntimeError("incomplete benchmark samples")
                elapsed = int(values["elapsed_ns"])
                case["runs"].append({"round": index + 1, "mode": mode, "version": values["version"],
                    "elapsed_ns": elapsed, "operations_per_second": len(samples) * 1e9 / elapsed,
                    "samples_ns": samples, "p50_ns": percentile(samples, .5),
                    "p95_ns": percentile(samples, .95), "p99_ns": percentile(samples, .99),
                    "max_ns": max(samples), "noise": noise})
        case["summary"] = {}
        for mode in modes:
            runs = [run for run in case["runs"] if run["mode"] == mode]
            case["summary"][mode] = {
                "median_ops_per_second": statistics.median(run["operations_per_second"] for run in runs),
                "median_p50_ns": statistics.median(run["p50_ns"] for run in runs),
                "median_p99_ns": statistics.median(run["p99_ns"] for run in runs),
                "min_ops_per_second": min(run["operations_per_second"] for run in runs),
                "max_ops_per_second": max(run["operations_per_second"] for run in runs),
            }
        report["cases"].append(case)
    with open(args.output, "x", encoding="utf-8") as output:
        json.dump(report, output, indent=2)
        output.write("\n")
    for case in report["cases"]:
        print(f"width={case['width']} summary={case['summary']}")


if __name__ == "__main__":
    main()
