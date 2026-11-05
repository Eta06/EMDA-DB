#!/usr/bin/env python3
"""Compare a finite same-element JSON array scan on SQLite and the prototype."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import time

from benchmarks.telemetry import difference, quiet_window, read, snapshot


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", required=True)
    parser.add_argument("--binary", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--rounds", type=int, default=4)
    parser.add_argument("--cycles", type=int, default=100)
    args = parser.parse_args()
    cpus = sorted(os.sched_getaffinity(0))
    if (not Path(args.directory).is_dir() or not Path(args.binary).is_file()
            or os.path.lexists(args.output) or len(cpus) < 2
            or not 2 <= args.rounds <= 50 or not 1 <= args.cycles <= 100000):
        parser.error("invalid paths, CPU availability, rounds or cycles")
    benchmark_cpu, driver_cpu = cpus[0], cpus[-1]
    os.sched_setaffinity(0, {driver_cpu})
    cases = [(100, 10), (1000, 10), (1000, 100)]
    report = {
        "revision": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "git_status": subprocess.check_output(["git", "status", "--short"], text=True),
        "binary_sha256": sha(args.binary), "script_sha256": sha(__file__),
        "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "platform": platform.platform(), "python": sys.version,
        "cpu_model": next((line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines() if line.startswith("model name")), "unknown"),
        "cpu_quota": read("/sys/fs/cgroup/cpu.max"), "benchmark_cpu": benchmark_cpu, "driver_cpu": driver_cpu,
        "filesystem": json.loads(subprocess.check_output(["findmnt", "-J", "-T", args.directory, "-o", "TARGET,FSTYPE"], text=True)),
        "scope": "Single client, in-process Rust and SQLite JSON1. Both perform a complete scan of small JSON documents with two object elements, requiring color and quantity to match the same object. Each returned document is parsed and checked. Values and predicates are chosen so both engines agree; type systems and encodings differ. SQLite uses WAL/FULL; the prototype uses its synchronized witnessed store. Setup and full reopen verification are outside timers but inside telemetry. No concurrent clients, writes during timing, multikey indexing, physical power loss, off-host restore, or universal ranking is measured.",
        "cases": cases, "rounds": args.rounds, "cycles": args.cycles, "runs": [], "failures": [],
    }
    output = Path(args.output)
    with output.open("x", encoding="utf-8") as stream:
        json.dump(report, stream, indent=2)
        stream.write("\n")
    for round_index in range(args.rounds):
        rotation = round_index % len(cases)
        for documents, matches in cases[rotation:] + cases[:rotation]:
            entry = {"documents": documents, "matches": matches, "round": round_index + 1}
            try:
                entry["pre_run_quiet"] = quiet_window(seconds=.25, attempts=3)
                before = snapshot()
                try:
                    command = ["taskset", "-c", str(benchmark_cpu), args.binary, args.directory, str(documents), str(matches), str(args.cycles)]
                    completed = subprocess.run(command, capture_output=True, text=True, timeout=300, check=True)
                    result = json.loads(completed.stdout)
                    if (result.get("documents") != documents or result.get("matches") != matches
                            or result.get("cycles") != args.cycles or result.get("reopen_verified") is not True
                            or result.get("sqlite_journal") != "wal" or result.get("sqlite_synchronous") != 2
                            or not any("SCAN d" in p for p in result.get("sqlite_query_plan", []))):
                        raise ValueError("workload, query plan or recovery mismatch")
                    for variant in ("prototype_scan", "sqlite_scan"):
                        values = result.get("samples_ns", {}).get(variant)
                        if not isinstance(values, list) or len(values) != args.cycles or any(type(v) is not int or v < 0 for v in values):
                            raise ValueError(f"invalid samples for {variant}")
                    entry["measurement"] = result
                    entry["command"] = command
                finally:
                    entry["noise"] = difference(before, snapshot())
            except Exception as error:
                entry["error"] = f"{type(error).__name__}: {error}"
                report["failures"].append({"documents": documents, "matches": matches, "round": round_index + 1, "error": entry["error"]})
            finally:
                report["runs"].append(entry)
                output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    if report["failures"]:
        raise SystemExit("one or more array comparison runs failed; see output")
    print(f"Completed {len(report['runs'])} verified array comparison runs")


if __name__ == "__main__":
    main()
