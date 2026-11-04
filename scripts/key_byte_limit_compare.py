#!/usr/bin/env python3
"""Compare durable point operations with and without the live-key byte cap."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import statistics
import subprocess
import time

from benchmarks.telemetry import difference, quiet_window, read, snapshot

MODES = ("prototype", "prototype-key-byte-limit")


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def parse_line(line):
    return dict(item.split("=", 1) for item in line.split() if "=" in item)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--binary", default="target/release/examples/kv_bench")
    parser.add_argument("--baseline-binary")
    parser.add_argument("--baseline-revision")
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--operations", type=int, default=200)
    args = parser.parse_args()
    if (not 2 <= args.rounds <= 50 or not 20 <= args.operations <= 10000
            or not Path(args.directory).is_dir() or not Path(args.binary).is_file()
            or os.path.lexists(args.output)):
        parser.error("existing directory and binary, new output path, valid rounds and operations required")
    if bool(args.baseline_binary) != bool(args.baseline_revision):
        parser.error("baseline binary and revision must be provided together")
    if args.baseline_binary and not Path(args.baseline_binary).is_file():
        parser.error("baseline binary must exist")
    allowed = sorted(os.sched_getaffinity(0))
    if len(allowed) < 2:
        parser.error("at least two allowed CPUs are required")
    benchmark_cpus, driver_cpu = allowed[:min(4, len(allowed) - 1)], allowed[-1]
    os.sched_setaffinity(0, {driver_cpu})
    report = {
        "revision": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "git_status": subprocess.check_output(["git", "status", "--short"], text=True),
        "binary_sha256": sha256(args.binary),
        "example_sha256": sha256("examples/kv_bench.rs"),
        "driver_sha256": sha256(__file__),
        "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "cpu_model": next((line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines() if line.startswith("model name")), "unknown"),
        "cpu_quota": read("/sys/fs/cgroup/cpu.max"),
        "allowed_cpus": allowed, "benchmark_cpus": benchmark_cpus, "driver_cpu": driver_cpu,
        "filesystem": json.loads(subprocess.check_output(["findmnt", "-J", "-T", args.directory, "-o", "TARGET,FSTYPE"], text=True)),
        "limitations": "One VM and filesystem, direct embedded APIs, warm cache, 16 pre-existing eight-byte keys and 64-byte values. The cap is exactly 128 live-key bytes; workloads update existing keys and do not measure insertion or rejection. Each acknowledged write synchronizes under the same prototype durability contract; capped mutations serialize while uncapped writes may share syncs. The example verifies sampled values after reopen. Latency percentiles are per-run summaries, not individual samples. VM/cgroup noise includes fixture setup, reopen and cleanup. No power-loss validation or general ranking.",
        "cases": [],
    }
    variants = [(mode, args.binary, mode) for mode in MODES]
    if args.baseline_binary:
        report["baseline_revision"] = subprocess.check_output(
            ["git", "rev-parse", "--verify", args.baseline_revision + "^{commit}"], text=True
        ).strip()
        report["baseline_binary_sha256"] = sha256(args.baseline_binary)
        report["binary_provenance"] = "Baseline revision is caller-supplied; binary hashes are recorded. Build each from its corresponding clean source."
        variants.insert(0, ("baseline-key-byte-limit", args.baseline_binary, "prototype-key-byte-limit"))
    cpus = ",".join(map(str, benchmark_cpus))
    for clients in (1, 4):
        for write_percent in (50, 100):
            case = {"clients": clients, "write_percent": write_percent, "keys": 16,
                    "operations_per_client": args.operations, "quiet_check": quiet_window(), "runs": []}
            for round_index in range(args.rounds):
                rotation = round_index % len(variants)
                order = variants[rotation:] + variants[:rotation]
                for mode, binary, backend in order:
                    before = snapshot()
                    command = ["taskset", "-c", cpus, binary, backend, str(clients), str(args.operations),
                               "16", str(write_percent), args.directory, "64"]
                    process = subprocess.run(command, check=True, capture_output=True, text=True, timeout=300)
                    noise = difference(before, snapshot())
                    lines = process.stdout.strip().splitlines()
                    head = parse_line(lines[0])
                    latency = {line.split(":", 1)[0]: parse_line(line) for line in lines if line.startswith(("read:", "write:"))}
                    if (head.get("backend") != backend or int(head.get("completed", 0)) != clients * args.operations
                            or "verified_reopen=true" not in lines or int(latency["write"]["count"]) == 0):
                        raise RuntimeError("invalid benchmark output or failed reopen verification")
                    case["runs"].append({"round": round_index + 1, "mode": mode,
                                         "operations_per_second": float(head["ops_per_sec"]),
                                         "latency": latency, "stdout": process.stdout, "noise": noise})
            case["summary"] = {}
            for mode, _, _ in variants:
                runs = [run for run in case["runs"] if run["mode"] == mode]
                speeds = [run["operations_per_second"] for run in runs]
                case["summary"][mode] = {
                    "median_ops_per_second": statistics.median(speeds),
                    "min_ops_per_second": min(speeds), "max_ops_per_second": max(speeds),
                    "median_write_p50_us": statistics.median(float(run["latency"]["write"]["p50_us"]) for run in runs),
                    "median_write_p99_us": statistics.median(float(run["latency"]["write"]["p99_us"]) for run in runs),
                }
            report["cases"].append(case)
    with open(args.output, "x", encoding="utf-8") as output:
        json.dump(report, output, indent=2)
        output.write("\n")
    for case in report["cases"]:
        print(f"clients={case['clients']} write_percent={case['write_percent']} summary={case['summary']}")


if __name__ == "__main__":
    main()
