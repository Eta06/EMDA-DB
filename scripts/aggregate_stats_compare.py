#!/usr/bin/env python3
"""Measure checked grouped integer statistics against a SQLite table scan."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import statistics
import subprocess
import time

from benchmarks.telemetry import difference, quiet_window, read, snapshot


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def validate(result, documents, cycles):
    if (result.get("documents") != documents or result.get("cycles") != cycles
            or result.get("prototype_reopen_verified") is not True or result.get("groups") != min(8, documents)
            or result.get("sqlite_journal") != "wal" or result.get("sqlite_synchronous") != 2
            or "SCAN docs" not in result.get("sqlite_query_plan", "")
            or not result.get("sqlite_version")):
        raise ValueError("workload identity, SQLite settings or correctness checks failed")
    for name in ("prototype", "sqlite"):
        samples = result.get("samples_ns", {}).get(name)
        if (not isinstance(samples, list) or len(samples) != cycles
                or any(type(value) is not int or value < 0 for value in samples)):
            raise ValueError(f"invalid samples for {name}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", required=True)
    parser.add_argument("--binary", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--documents", default="64,256")
    parser.add_argument("--rounds", type=int, default=4)
    parser.add_argument("--cycles", type=int, default=30)
    args = parser.parse_args()
    try:
        cases = [int(value) for value in args.documents.split(",")]
    except ValueError:
        parser.error("document counts must be integers")
    cpus = sorted(os.sched_getaffinity(0))
    if (not cases or len(set(cases)) != len(cases) or any(not 1 <= case <= 256 for case in cases)
            or not 2 <= args.rounds <= 50 or not 1 <= args.cycles <= 100000
            or not Path(args.directory).is_dir() or not Path(args.binary).is_file()
            or os.path.lexists(args.output) or len(cpus) < 2):
        parser.error("invalid paths, workload limits, rounds or CPU availability")
    benchmark_cpu, driver_cpu = cpus[0], cpus[-1]
    os.sched_setaffinity(0, {driver_cpu})
    report = {
        "revision": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "git_status": subprocess.check_output(["git", "status", "--short"], text=True),
        "binary_sha256": sha(args.binary), "script_sha256": sha(__file__),
        "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "platform": platform.platform(), "cpu_quota": read("/sys/fs/cgroup/cpu.max"),
        "cpu_model": next((line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines() if line.startswith("model name")), "unknown"),
        "benchmark_cpu": benchmark_cpu, "driver_cpu": driver_cpu,
        "filesystem": json.loads(subprocess.check_output(["findmnt", "-J", "-T", args.directory, "-o", "TARGET,FSTYPE"], text=True)),
        "scope": "Single-client in-process grouped count, sum, minimum and maximum of two signed integer fields over full scans. SQLite uses normalized columns and a prepared GROUP BY statement; the prototype reads JSON documents and uses a transaction. Results are checked against each other after every timed query. Each cycle alternates execution order after five warmups; setup and reopening are outside timers. No concurrency, write throughput, physical power loss, independent restore or general ranking is measured.",
        "cases": cases, "rounds": args.rounds, "cycles": args.cycles, "runs": [], "failures": [],
    }
    output = Path(args.output)
    with output.open("x", encoding="utf-8") as file:
        json.dump(report, file, indent=2)
        file.write("\n")

    def save():
        output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")

    for round_index in range(args.rounds):
        rotation = round_index % len(cases)
        for documents in cases[rotation:] + cases[:rotation]:
            entry = {"documents": documents, "round": round_index + 1}
            try:
                entry["pre_run_quiet"] = quiet_window(seconds=.25, attempts=3)
                before = snapshot()
                try:
                    command = ["taskset", "-c", str(benchmark_cpu), args.binary, args.directory, str(documents), str(args.cycles)]
                    completed = subprocess.run(command, capture_output=True, text=True, timeout=300)
                    if completed.returncode:
                        raise RuntimeError(f"benchmark exited {completed.returncode}: {completed.stderr[-4096:]}")
                    result = json.loads(completed.stdout)
                    validate(result, documents, args.cycles)
                    entry["measurement"] = result
                    entry["command"] = command
                finally:
                    entry["noise"] = difference(before, snapshot())
            except Exception as error:
                entry["error"] = f"{type(error).__name__}: {error}"
                report["failures"].append({"documents": documents, "round": round_index + 1, "error": entry["error"]})
            finally:
                report["runs"].append(entry)
                save()
    if report["failures"]:
        raise SystemExit("one or more comparison runs failed; see output")
    report["summary"] = []
    for documents in cases:
        runs = [run for run in report["runs"] if run["documents"] == documents]
        variants = {}
        for name in ("prototype", "sqlite"):
            medians = [statistics.median(run["measurement"]["samples_ns"][name]) / 1000 for run in runs]
            variants[name] = {"median_of_round_medians_us": statistics.median(medians), "round_median_min_us": min(medians), "round_median_max_us": max(medians)}
        report["summary"].append({"documents": documents, "variants": variants})
    save()
    print(f"Completed {len(report['runs'])} verified comparison runs")


if __name__ == "__main__":
    main()
