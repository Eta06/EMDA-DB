#!/usr/bin/env python3
"""Paired before/after runs of the checked array-element comparison binary."""
import argparse
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import time

from array_element_compare import sha
from benchmarks.telemetry import difference, quiet_window, read, snapshot


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--directory", required=True)
    p.add_argument("--before", required=True)
    p.add_argument("--after", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--rounds", type=int, default=4)
    p.add_argument("--cycles", type=int, default=100)
    a = p.parse_args()
    cpus = sorted(os.sched_getaffinity(0))
    if (not Path(a.directory).is_dir() or not Path(a.before).is_file() or not Path(a.after).is_file()
            or os.path.lexists(a.output) or len(cpus) < 2 or not 2 <= a.rounds <= 50
            or not 1 <= a.cycles <= 100000):
        p.error("invalid paths, CPU availability, rounds or cycles")
    worker_cpu, driver_cpu = cpus[0], cpus[-1]
    os.sched_setaffinity(0, {driver_cpu})
    cases = [(100, 10), (1000, 10), (1000, 100)]
    report = {
        "revision": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "git_status": subprocess.check_output(["git", "status", "--short"], text=True),
        "before_binary_sha256": sha(a.before), "after_binary_sha256": sha(a.after), "script_sha256": sha(__file__),
        "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "platform": platform.platform(), "python": sys.version, "cpu_quota": read("/sys/fs/cgroup/cpu.max"),
        "worker_cpu": worker_cpu, "driver_cpu": driver_cpu,
        "filesystem": json.loads(subprocess.check_output(["findmnt", "-J", "-T", a.directory, "-o", "TARGET,FSTYPE"], text=True)),
        "scope": "Rotated before/after fresh-process runs, each containing alternating prototype and SQLite JSON1 complete scans of the same two-element object arrays. Every returned document and all data after reopen are verified. Both binaries run locally in process, with SQLite WAL/FULL; their representations and semantics are not identical for arbitrary JSON. No concurrent workload, power loss, off-host recovery or universal ranking is measured.",
        "cases": cases, "rounds": a.rounds, "cycles": a.cycles, "runs": [], "failures": []}
    output = Path(a.output)
    with output.open("x", encoding="utf-8") as stream:
        json.dump(report, stream, indent=2)
        stream.write("\n")
    for round_index in range(a.rounds):
        rotation = round_index % len(cases)
        for case_index, (documents, matches) in enumerate(cases[rotation:] + cases[:rotation]):
            variants = [("before", a.before), ("after", a.after)]
            if (round_index + case_index) % 2:
                variants.reverse()
            for variant, binary in variants:
                entry = {"variant": variant, "documents": documents, "matches": matches, "round": round_index + 1}
                try:
                    entry["pre_run_quiet"] = quiet_window(seconds=.25, attempts=3)
                    before = snapshot()
                    try:
                        command = ["taskset", "-c", str(worker_cpu), binary, a.directory, str(documents), str(matches), str(a.cycles)]
                        completed = subprocess.run(command, capture_output=True, text=True, timeout=300, check=True)
                        result = json.loads(completed.stdout)
                        if (result.get("documents") != documents or result.get("matches") != matches or result.get("cycles") != a.cycles
                                or result.get("reopen_verified") is not True or result.get("sqlite_journal") != "wal"
                                or result.get("sqlite_synchronous") != 2
                                or not any("SCAN d" in value for value in result.get("sqlite_query_plan", []))):
                            raise ValueError("invalid workload, recovery, or SQLite scan plan")
                        for label in ("prototype_scan", "sqlite_scan"):
                            values = result.get("samples_ns", {}).get(label)
                            if not isinstance(values, list) or len(values) != a.cycles or any(type(v) is not int or v < 0 for v in values):
                                raise ValueError(f"invalid {label} samples")
                        entry["measurement"] = result
                        entry["command"] = command
                    finally:
                        entry["noise"] = difference(before, snapshot())
                except Exception as error:
                    entry["error"] = f"{type(error).__name__}: {error}"
                    report["failures"].append({"variant": variant, "documents": documents, "matches": matches,
                                               "round": round_index + 1, "error": entry["error"]})
                finally:
                    report["runs"].append(entry)
                    output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    if report["failures"]:
        raise SystemExit("one or more runs failed; see output")
    print(f"Completed {len(report['runs'])} checked paired array scans")


if __name__ == "__main__":
    main()
