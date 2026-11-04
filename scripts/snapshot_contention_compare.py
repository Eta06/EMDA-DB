#!/usr/bin/env python3
"""Compare snapshot implementations under closed-loop read/write contention."""
import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import time

from benchmarks.telemetry import difference, quiet_window, read, snapshot


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def validate(result, keys, writers, operations):
    if (result.get("base_keys") != keys or result.get("writers") != writers
            or result.get("operations_per_writer") != operations or result.get("reopen_verified") is not True
            or result.get("log_format") not in ("V2", "V3") or result.get("reader_sampling") != "bounded reservoir"):
        raise ValueError("invalid workload identity, log format or recovery result")
    writer = result.get("writer_samples_ns", [])
    reader = result.get("reader_samples_ns", [])
    if (len(writer) != writers * operations or not reader
            or len(reader) > result.get("reader_sample_cap", 0)
            or len(reader) > result.get("reader_reads", 0)
            or result.get("reader_max_ns", -1) < max(reader)
            or any(type(sample) is not int or sample < 0 for sample in writer + reader)
            or type(result.get("elapsed_ns")) is not int or result["elapsed_ns"] <= 0):
        raise ValueError("incomplete or invalid timing samples")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", required=True)
    parser.add_argument("--before-binary", required=True)
    parser.add_argument("--after-binary", required=True)
    parser.add_argument("--output", required=True, help="new gzip-compressed JSON path")
    parser.add_argument("--base-keys", type=int, default=9000)
    parser.add_argument("--writers", type=int, default=4)
    parser.add_argument("--operations", type=int, default=100)
    parser.add_argument("--rounds", type=int, default=5)
    args = parser.parse_args()
    available = sorted(os.sched_getaffinity(0))
    if (not Path(args.directory).is_dir() or not Path(args.before_binary).is_file()
            or not Path(args.after_binary).is_file() or os.path.lexists(args.output)
            or len(available) < 2 or not 1 <= args.base_keys <= 10000
            or not 1 <= args.writers <= 8 or not 1 <= args.operations <= 1000
            or not 2 <= args.rounds <= 50):
        parser.error("invalid paths, CPU availability or workload settings")
    cpus, driver_cpu = available[:min(3, len(available)-1)], available[-1]
    os.sched_setaffinity(0, {driver_cpu})
    report = {
        "revision": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "git_status": subprocess.check_output(["git", "status", "--short"], text=True),
        "before_binary_sha256": sha256(args.before_binary), "after_binary_sha256": sha256(args.after_binary),
        "script_sha256": sha256(__file__), "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "platform": platform.platform(), "cpu_quota": read("/sys/fs/cgroup/cpu.max"),
        "cpu_model": next((line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines() if line.startswith("model name")), "unknown"),
        "benchmark_cpus": cpus, "driver_cpu": driver_cpu,
        "filesystem": json.loads(subprocess.check_output(["findmnt", "-J", "-T", args.directory, "-o", "TARGET,FSTYPE"], text=True)),
        "scope": "One VM and one embedded store, closed-loop workload, four or fewer pinned CPUs. Independent writers retry conservative two-key transactions while one reader repeatedly reads a stable key. The reader keeps a bounded reservoir of the whole run and records the maximum separately; its percentiles are estimates when sampling occurs. Preparation, final verification and cleanup are outside operation timers but inside VM/cgroup telemetry. No offered-load, total-memory, physical power-loss or general competitor claim is established.",
        "base_keys": args.base_keys, "writers": args.writers, "operations_per_writer": args.operations,
        "rounds": args.rounds, "runs": [], "failures": [],
    }
    output = Path(args.output)
    with output.open("xb"):
        pass

    def save():
        with gzip.open(output, "wt", encoding="utf-8", compresslevel=6) as file:
            json.dump(report, file, separators=(",", ":"))
            file.write("\n")

    targets = [("before", args.before_binary), ("after", args.after_binary)]
    for round_index in range(args.rounds):
        order = targets if round_index % 2 == 0 else list(reversed(targets))
        for variant, binary in order:
            entry = {"variant": variant, "round": round_index + 1}
            try:
                entry["pre_run_quiet"] = quiet_window(seconds=.25, attempts=3)
                before = snapshot()
                try:
                    command = ["taskset", "-c", ",".join(map(str, cpus)), binary, args.directory,
                               str(args.base_keys), str(args.writers), str(args.operations)]
                    completed = subprocess.run(command, capture_output=True, text=True, timeout=300)
                    if completed.returncode:
                        raise RuntimeError(f"benchmark exited {completed.returncode}: {completed.stderr[-4096:]}")
                    result = json.loads(completed.stdout)
                    validate(result, args.base_keys, args.writers, args.operations)
                    entry["measurement"] = result
                    entry["command"] = command
                finally:
                    entry["noise"] = difference(before, snapshot())
            except Exception as error:
                entry["error"] = f"{type(error).__name__}: {error}"
                report["failures"].append({"variant": variant, "round": round_index + 1, "error": entry["error"]})
            finally:
                report["runs"].append(entry)
                save()
    if report["failures"]:
        raise SystemExit("one or more contention runs failed; see output")
    print(f"Completed {len(report['runs'])} contention runs")


if __name__ == "__main__":
    main()
