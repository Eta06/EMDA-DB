#!/usr/bin/env python3
"""Compare complete, synchronized incremental collection deletion steps."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import time

from benchmarks.telemetry import difference, quiet_window, read, snapshot


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def validate(data, mode, count):
    if (data.get("mode") != mode or data.get("documents") != count
            or data.get("removed") != 3 * count or data.get("recovery_verified") is not True
            or data.get("log_format") not in ("EMDALOG2", "EMDALOG3")
            or type(data.get("log_bytes_before")) is not int
            or type(data.get("log_bytes_after")) is not int
            or data["log_bytes_after"] <= data["log_bytes_before"]):
        raise ValueError("invalid workload identity or recovery verification")
    samples = data.get("samples", [])
    if (not samples or sum(s["removed"] for s in samples) != 3 * count
            or sum(s["complete"] is True for s in samples) != 1
            or samples[-1]["complete"] is not True or samples[-1]["removed"] != 0
            or any(type(s["ns"]) is not int or s["ns"] < 0
                   or type(s["removed"]) is not int or not 0 <= s["removed"] <= 128
                   for s in samples)):
        raise ValueError("incomplete deletion timing samples")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", required=True)
    parser.add_argument("--before-binary", required=True)
    parser.add_argument("--after-binary", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--sizes", default="100,1000,3000")
    parser.add_argument("--rounds", type=int, default=4)
    args = parser.parse_args()
    try:
        sizes = [int(n) for n in args.sizes.split(",")]
    except ValueError:
        parser.error("invalid sizes")
    cpus = sorted(os.sched_getaffinity(0))
    if (not Path(args.directory).is_dir() or not Path(args.before_binary).is_file()
            or not Path(args.after_binary).is_file() or os.path.lexists(args.output)
            or len(cpus) < 2 or not sizes or any(not 1 <= n <= 4000 for n in sizes)
            or not 2 <= args.rounds <= 50):
        parser.error("invalid workload settings, paths or CPU availability")
    benchmark_cpus, driver_cpu = cpus[:min(3, len(cpus) - 1)], cpus[-1]
    os.sched_setaffinity(0, {driver_cpu})
    output = Path(args.output)
    report = {
        "base_revision": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "git_status": subprocess.check_output(["git", "status", "--short"], text=True),
        "before_binary_sha256": sha(args.before_binary), "after_binary_sha256": sha(args.after_binary),
        "example_sha256": sha("examples/collection_drop_bench.rs"), "script_sha256": sha(__file__),
        "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "platform": platform.platform(), "cpu_quota": read("/sys/fs/cgroup/cpu.max"),
        "cpu_model": next((line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines() if line.startswith("model name")), "unknown"),
        "benchmark_cpus": benchmark_cpus, "driver_cpu": driver_cpu,
        "filesystem": json.loads(subprocess.check_output(["findmnt", "-J", "-T", args.directory, "-o", "TARGET,FSTYPE"], text=True)),
        "scope": "One VM, one client; each step removes up to 128 document/index keys in synchronized transactions. Preparation, recreation and reopen verification are outside operation timers but included in VM/cgroup telemetry. Cursor candidate verifies all namespaces empty before metadata deletion. Not a concurrency, total-memory, physical-power-loss, off-host or cross-engine comparison.",
        "sizes": sizes, "rounds": args.rounds, "runs": [], "failures": [],
    }
    with output.open("x"):
        pass

    def save():
        output.write_text(json.dumps(report, indent=2) + "\n")

    variants = [("before", args.before_binary), ("after", args.after_binary)]
    for round_index in range(args.rounds):
        for count in sizes[round_index % len(sizes):] + sizes[:round_index % len(sizes)]:
            for mode in ("ordinary", "checkpoint"):
                for variant, binary in (variants if round_index % 2 == 0 else reversed(variants)):
                    entry = {"round": round_index + 1, "documents": count, "mode": mode, "variant": variant}
                    try:
                        entry["pre_run_quiet"] = quiet_window(seconds=.25, attempts=3)
                        before = snapshot()
                        try:
                            command = ["taskset", "-c", ",".join(map(str, benchmark_cpus)), binary, args.directory, mode, str(count)]
                            completed = subprocess.run(command, capture_output=True, text=True, timeout=300, check=True)
                            entry["measurement"] = json.loads(completed.stdout)
                            validate(entry["measurement"], mode, count)
                            entry["command"] = command
                        finally:
                            entry["noise"] = difference(before, snapshot())
                    except Exception as error:
                        entry["error"] = f"{type(error).__name__}: {error}"
                        report["failures"].append({"round": round_index + 1, "mode": mode, "variant": variant, "documents": count, "error": entry["error"]})
                    finally:
                        report["runs"].append(entry)
                        save()
    if report["failures"]:
        raise SystemExit("one or more benchmark runs failed; see output")
    print(f"Completed {len(report['runs'])} deletion runs")


if __name__ == "__main__":
    main()
