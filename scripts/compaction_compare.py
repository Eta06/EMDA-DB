#!/usr/bin/env python3
"""Compare disposable verified compactions using two separately built binaries."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import statistics
import subprocess
import time

from benchmarks.telemetry import difference, quiet_window, read, snapshot


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--before-binary", required=True)
    parser.add_argument("--after-binary", required=True)
    parser.add_argument("--before-revision", required=True)
    parser.add_argument("--after-revision", required=True)
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--keys", default="200,5000")
    args = parser.parse_args()
    try:
        counts = [int(value) for value in args.keys.split(",")]
    except ValueError:
        parser.error("keys must be comma-separated integers")
    if not counts or len(set(counts)) != len(counts) or any(not 16 <= n <= 100_000 for n in counts) or not 2 <= args.rounds <= 50:
        parser.error("invalid key counts or rounds")
    if not Path(args.directory).is_dir() or not all(Path(path).is_file() for path in [args.before_binary, args.after_binary]):
        parser.error("existing directory and two built binaries are required")
    if os.path.lexists(args.output):
        parser.error("output must not already exist")
    revisions = {name: subprocess.check_output(["git", "rev-parse", "--verify", revision + "^{commit}"], text=True).strip()
                 for name, revision in [("before", args.before_revision), ("after", args.after_revision)]}
    allowed = sorted(os.sched_getaffinity(0))
    if len(allowed) < 2:
        parser.error("at least two allowed CPUs are required")
    benchmark_cpu, driver_cpu = allowed[0], allowed[-1]
    os.sched_setaffinity(0, {driver_cpu})
    binaries = {"before": args.before_binary, "after": args.after_binary}
    report = {
        "revisions": revisions,
        "binary_sha256": {name: sha256(path) for name, path in binaries.items()},
        "binary_provenance": "Revisions are caller-supplied; binary hashes are recorded. Build each from its own clean checkout and empty target directory.",
        "driver_revision": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "driver_status": subprocess.check_output(["git", "status", "--short"], text=True),
        "driver_sha256": sha256(__file__), "example_sha256": sha256("examples/compaction_bench.rs"),
        "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "cpu_model": next((line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines() if line.startswith("model name")), "unknown"),
        "cpu_quota": read("/sys/fs/cgroup/cpu.max"), "allowed_cpus": allowed,
        "benchmark_cpu": benchmark_cpu, "driver_cpu": driver_cpu,
        "filesystem": json.loads(subprocess.check_output(["findmnt", "-J", "-T", args.directory, "-o", "TARGET,FSTYPE"], text=True)),
        "limitations": "One VM and filesystem, warm source immediately after preparation, small values, sequential separate-copy compaction. Each run creates its own synthetic fixture and verifies selected values. VM/cgroup noise includes fixture setup and cleanup. No physical power-loss validation or general competitor ranking.",
        "cases": [],
    }
    for count in counts:
        case = {"keys": count, "quiet_check": quiet_window(), "runs": []}
        for round_index in range(args.rounds):
            modes = ("before", "after") if round_index % 2 == 0 else ("after", "before")
            for mode in modes:
                before = snapshot()
                command = ["taskset", "-c", str(benchmark_cpu), binaries[mode], args.directory, str(count)]
                process = subprocess.run(command, check=True, capture_output=True, text=True, timeout=300)
                noise = difference(before, snapshot())
                values = dict(item.split("=", 1) for item in process.stdout.strip().split())
                if int(values["keys"]) != count:
                    raise RuntimeError("unexpected compaction workload")
                case["runs"].append({"round": round_index + 1, "mode": mode,
                    "live_keys": int(values["live"]), "source_bytes": int(values["source_bytes"]),
                    "compact_bytes": int(values["compact_bytes"]), "compact_us": int(values["compact_us"]),
                    "noise": noise})
        case["summary"] = {mode: {
            "median_compact_us": statistics.median(r["compact_us"] for r in case["runs"] if r["mode"] == mode),
            "min_compact_us": min(r["compact_us"] for r in case["runs"] if r["mode"] == mode),
            "max_compact_us": max(r["compact_us"] for r in case["runs"] if r["mode"] == mode),
        } for mode in binaries}
        report["cases"].append(case)
    with open(args.output, "x", encoding="utf-8") as output:
        json.dump(report, output, indent=2)
        output.write("\n")
    for case in report["cases"]:
        print(f"keys={case['keys']} summary={case['summary']}")


if __name__ == "__main__":
    main()
