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


def validate(result, documents, cycles, selective=False, membership=False):
    expected_groups = min(2 if membership else 1 if selective else 8, documents)
    expected_sqlite = "SEARCH docs USING INDEX docs_g" if selective else "SCAN docs"
    matched = (documents + 7) // 8 + ((documents + 6) // 8 if membership else 0)
    if (result.get("documents") != documents or result.get("cycles") != cycles
            or result.get("prototype_reopen_verified") is not True or result.get("groups") != expected_groups
            or result.get("selective", False) != selective
            or result.get("membership", False) != membership
            or (selective and result.get("matched") != matched)
            or result.get("sqlite_journal") != "wal" or result.get("sqlite_synchronous") != 2
            or expected_sqlite not in result.get("sqlite_query_plan", "")
            or not result.get("sqlite_version")):
        raise ValueError("workload identity, SQLite settings or correctness checks failed")
    for name in ("prototype", "planned", "sqlite") if selective else ("prototype", "sqlite"):
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
    layout = parser.add_mutually_exclusive_group()
    layout.add_argument("--selective", action="store_true", help="compare equality-indexed grouped statistics")
    layout.add_argument("--membership", action="store_true", help="compare two groups selected by membership")
    args = parser.parse_args()
    selective = args.selective or args.membership
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
        "scope": ("Single-client in-process grouped count, sum, minimum and maximum of two signed integer fields. "
                  + ("One or two of eight groups are selected. Compare EMDA full scan, EMDA index probes, and SQLite indexed GROUP BY. "
                     if selective else "Both engines perform full scans. ")
                  + "SQLite uses normalized columns and a prepared statement. EMDA reads JSON documents in a transaction. "
                    "Every timed query is checked against the other engine. Execution order rotates after five warmups. "
                    "Setup and reopening are outside timers."),
        "selective": selective,
        "membership": args.membership,
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
                    if selective:
                        command.append("membership" if args.membership else "selective")
                    entry["command"] = command
                    completed = subprocess.run(command, capture_output=True, text=True, timeout=300)
                    entry["exit_code"] = completed.returncode
                    entry["stdout"] = completed.stdout
                    entry["stderr"] = completed.stderr
                    if completed.returncode:
                        raise RuntimeError(f"benchmark exited {completed.returncode}: {completed.stderr[-4096:]}")
                    result = json.loads(completed.stdout)
                    validate(result, documents, args.cycles, selective, args.membership)
                    entry["measurement"] = result
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
        for name in ("prototype", "planned", "sqlite") if selective else ("prototype", "sqlite"):
            medians = [statistics.median(run["measurement"]["samples_ns"][name]) / 1000 for run in runs]
            variants[name] = {"median_of_round_medians_us": statistics.median(medians), "round_median_min_us": min(medians), "round_median_max_us": max(medians)}
        report["summary"].append({"documents": documents, "variants": variants})
    save()
    print(f"Completed {len(report['runs'])} verified comparison runs")


if __name__ == "__main__":
    main()
