#!/usr/bin/env python3
"""Compare bounded nonunique document queries with scan and SQLite baselines."""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import statistics
import subprocess
import sys
import time

from benchmarks.telemetry import difference, quiet_window, read, snapshot

VARIANTS = ("prototype_scan", "prototype_planned", "sqlite_scan", "sqlite_index")


def parse_cases(text):
    cases = []
    for item in text.split(","):
        parts = item.split(":")
        if len(parts) != 2:
            raise ValueError("each case must be DOCUMENTS:MATCHES")
        documents, matches = map(int, parts)
        if not 1 <= documents <= 1024 or not 1 <= matches <= min(documents, 256):
            raise ValueError("case exceeds supported document or match limits")
        cases.append((documents, matches))
    if not cases or len(set(cases)) != len(cases):
        raise ValueError("cases must be nonempty and unique")
    return cases


def validate(result, documents, matches, cycles, index_name="docs_group", membership=False,
             compound_membership=False):
    if (result.get("documents") != documents or result.get("matches") != matches
            or result.get("cycles") != cycles or result.get("reopen_verified") is not True
            or result.get("sqlite_journal") != "wal" or result.get("sqlite_synchronous") != 2
            or f"USING INDEX {index_name}" not in result.get("sqlite_query_plan", "")
            or "SCAN docs" not in result.get("sqlite_scan_plan", "")
            or "USING INDEX" in result.get("sqlite_scan_plan", "")
            or not result.get("sqlite_version")):
        raise ValueError("benchmark identity, configuration or recovery check failed")
    if membership and result.get("predicate") != "membership":
        raise ValueError("benchmark did not execute the requested membership predicate")
    if compound_membership and result.get("predicate") != "compound-membership":
        raise ValueError("benchmark did not execute the requested compound membership predicate")
    for variant in VARIANTS:
        samples = result.get("samples_ns", {}).get(variant)
        if not isinstance(samples, list) or len(samples) != cycles:
            raise ValueError(f"missing samples for {variant}")
        if any(type(value) is not int or value < 0 for value in samples):
            raise ValueError(f"invalid samples for {variant}")


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", required=True)
    parser.add_argument("--binary", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--cases", default="100:10,1000:10,1000:100")
    parser.add_argument("--index-name", choices=("docs_group", "docs_number", "docs_pair", "docs_partial", "docs_tenant_number"), default="docs_group")
    predicate = parser.add_mutually_exclusive_group()
    predicate.add_argument("--membership", action="store_true", help="expect the two-value membership workload")
    predicate.add_argument("--compound-membership", action="store_true", help="expect four complete compound membership probes")
    parser.add_argument("--rounds", type=int, default=4)
    parser.add_argument("--cycles", type=int, default=50)
    args = parser.parse_args()
    try:
        cases = parse_cases(args.cases)
    except ValueError as error:
        parser.error(str(error))
    cpus = sorted(os.sched_getaffinity(0))
    if (not Path(args.directory).is_dir() or not Path(args.binary).is_file()
            or os.path.lexists(args.output) or len(cpus) < 2
            or (args.membership and args.index_name != "docs_group")
            or (args.compound_membership and args.index_name != "docs_pair")
            or not 2 <= args.rounds <= 50 or not 1 <= args.cycles <= 100000):
        parser.error("invalid paths, CPU availability, rounds or cycle count")
    benchmark_cpu, driver_cpu = cpus[0], cpus[-1]
    os.sched_setaffinity(0, {driver_cpu})
    workload = {"docs_group": "equality", "docs_number": "greater-or-equal range with index order reversed relative to ID", "docs_pair": "complete two-field equality", "docs_partial": "partial equality guarded by a boolean predicate", "docs_tenant_number": "compound equality-prefix and scalar range"}[args.index_name]
    if args.membership:
        workload = "two-value membership, with both requested groups populated when at least two rows match"
    elif args.compound_membership:
        workload = "two two-value membership predicates on a complete compound tuple, with four distinct probes"
    report = {
        "revision": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "git_status": subprocess.check_output(["git", "status", "--short"], text=True),
        "binary_sha256": sha256(args.binary), "script_sha256": sha256(__file__),
        "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "platform": platform.platform(), "python": sys.version,
        "cpu_model": next((line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines() if line.startswith("model name")), "unknown"),
        "cpu_quota": read("/sys/fs/cgroup/cpu.max"), "benchmark_cpu": benchmark_cpu, "driver_cpu": driver_cpu,
        "filesystem": json.loads(subprocess.check_output(["findmnt", "-J", "-T", args.directory, "-o", "TARGET,FSTYPE"], text=True)),
        "scope": "One client; read-only nonunique JSON " + workload + ". Prototype complete scan and planned index access versus SQLite forced primary-table scan and expression-index access. Returned JSON is parsed and verified for every query on both systems. All four variants run in rotated order within one process after 10 warmup cycles. Setup and full reopen/content verification are outside timers but inside VM telemetry. Both datasets share a process; peak RSS is combined. No concurrency, write throughput, physical power loss, off-host recovery or universal ranking is measured.",
        "sqlite_index_name": args.index_name,
        "membership": args.membership,
        "compound_membership": args.compound_membership,
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
        for documents, matches in cases[rotation:] + cases[:rotation]:
            entry = {"documents": documents, "matches": matches, "round": round_index + 1}
            try:
                entry["pre_run_quiet"] = quiet_window(seconds=.25, attempts=3)
                before = snapshot()
                try:
                    command = ["taskset", "-c", str(benchmark_cpu), args.binary, args.directory,
                               str(documents), str(matches), str(args.cycles)]
                    completed = subprocess.run(command, capture_output=True, text=True, timeout=300)
                    if completed.returncode:
                        raise RuntimeError(f"benchmark exited {completed.returncode}: {completed.stderr[-4096:]}")
                    result = json.loads(completed.stdout)
                    validate(result, documents, matches, args.cycles, args.index_name,
                             args.membership, args.compound_membership)
                    entry["measurement"] = result
                    entry["command"] = command
                finally:
                    entry["noise"] = difference(before, snapshot())
            except Exception as error:
                entry["error"] = f"{type(error).__name__}: {error}"
                report["failures"].append({"documents": documents, "matches": matches,
                                           "round": round_index + 1, "error": entry["error"]})
            finally:
                report["runs"].append(entry)
                save()
    if report["failures"]:
        raise SystemExit("one or more comparison runs failed; see output")
    report["summary"] = []
    for documents, matches in cases:
        runs = [run for run in report["runs"] if run["documents"] == documents and run["matches"] == matches]
        stats = {}
        for variant in VARIANTS:
            medians = [statistics.median(run["measurement"]["samples_ns"][variant]) / 1000 for run in runs]
            if not all(math.isfinite(value) for value in medians):
                raise ValueError("non-finite summary")
            stats[variant] = {"median_of_round_medians_us": statistics.median(medians),
                              "round_median_min_us": min(medians), "round_median_max_us": max(medians)}
        report["summary"].append({"documents": documents, "matches": matches, "variants": stats})
    save()
    print(f"Completed {len(report['runs'])} verified comparison runs")


if __name__ == "__main__":
    main()
