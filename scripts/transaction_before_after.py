#!/usr/bin/env python3
"""Paired, verified transaction measurements for two separately built revisions."""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import statistics
import subprocess
import time

from benchmarks.telemetry import difference, quiet_window, read, snapshot

MODES = ("conservative", "readset", "readset-fifo", "checkpoint", "checkpoint-fifo",
         "checkpoint-readset", "checkpoint-readset-fifo")


def percentile(values, fraction):
    return sorted(values)[math.ceil(len(values) * fraction) - 1]


def parse_result(output, mode, clients, operations, workload):
    fields = dict(item.split("=", 1) for item in output.strip().split())
    if (fields.get("backend") != mode or fields.get("workload") != workload
            or int(fields["clients"]) != clients or int(fields["operations"]) != operations):
        raise ValueError("unexpected transaction workload")
    samples = [int(value) for value in fields["samples_ns"].split(",")]
    conflicts = [int(value) for value in fields["conflicts"].split(",")]
    elapsed = int(fields["elapsed_ns"])
    if (len(samples) != clients * operations or len(conflicts) != clients
            or elapsed <= 0 or any(value < 0 for value in samples + conflicts)):
        raise ValueError("invalid or incomplete benchmark samples")
    return {"version": fields["version"], "elapsed_ns": elapsed,
            "operations_per_second": len(samples) * 1e9 / elapsed,
            "conflicts_per_client": conflicts, "samples_ns": samples,
            "p50_ns": percentile(samples, .50), "p95_ns": percentile(samples, .95),
            "p99_ns": percentile(samples, .99), "max_ns": max(samples)}


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def revision(path):
    return subprocess.check_output(["git", "-C", str(path), "rev-parse", "HEAD"], text=True).strip()


def dirty(path):
    return subprocess.check_output(["git", "-C", str(path), "status", "--porcelain"], text=True).strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", required=True)
    parser.add_argument("--baseline-source", required=True)
    parser.add_argument("--baseline-binary", required=True)
    parser.add_argument("--candidate-binary", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--clients", default="1,4")
    parser.add_argument("--modes", default="conservative,readset,checkpoint,checkpoint-readset")
    parser.add_argument("--operations", type=int, default=100)
    parser.add_argument("--rounds", type=int, default=4)
    args = parser.parse_args()
    try:
        clients = [int(item) for item in args.clients.split(",")]
    except ValueError:
        parser.error("clients must be comma-separated integers")
    modes = args.modes.split(",")
    if (not clients or len(set(clients)) != len(clients) or any(not 1 <= n <= 8 for n in clients)
            or not modes or len(set(modes)) != len(modes) or any(mode not in MODES for mode in modes)
            or not 1 <= args.operations <= 10000 or not 2 <= args.rounds <= 50
            or not Path(args.directory).is_dir() or not Path(args.baseline_source).is_dir()
            or not Path(args.baseline_binary).is_file() or not Path(args.candidate_binary).is_file()
            or os.path.lexists(args.output)):
        parser.error("invalid benchmark paths, modes or workload")
    baseline_source = Path(args.baseline_source).resolve()
    candidate_source = Path.cwd().resolve()
    baseline_hash = sha256(args.baseline_binary)
    candidate_hash = sha256(args.candidate_binary)
    if baseline_hash == candidate_hash:
        parser.error("binaries are identical; verify isolated build provenance before measuring")
    if dirty(baseline_source) or dirty(candidate_source):
        parser.error("both source checkouts must be clean")
    allowed = sorted(os.sched_getaffinity(0))
    if len(allowed) < 2:
        parser.error("at least two CPUs must be available")
    benchmark_cpus, driver_cpu = allowed[:min(4, len(allowed) - 1)], allowed[-1]
    os.sched_setaffinity(0, {driver_cpu})
    report = {
        "baseline_revision": revision(baseline_source), "candidate_revision": revision(candidate_source),
        "baseline_binary_sha256": baseline_hash, "candidate_binary_sha256": candidate_hash,
        "script_sha256": sha256(__file__), "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "platform": platform.platform(), "cpu_quota": read("/sys/fs/cgroup/cpu.max"),
        "cpu_model": next((line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines() if line.startswith("model name")), "unknown"),
        "benchmark_cpus": benchmark_cpus, "driver_cpu": driver_cpu,
        "filesystem": json.loads(subprocess.check_output(["findmnt", "-J", "-T", args.directory, "-o", "TARGET,FSTYPE"], text=True)),
        "scope": "Paired warm-cache, in-process, two-key serializable transactions with a fresh fixture per run. The benchmark validates every counter after reopening. Both binaries use the same workload and alternate order. The candidate exercises existing transaction APIs without requesting cancellation. Different scheduling, filesystem and VM noise remain; no production latency, physical power loss or competitor ranking follows.",
        "clients": clients, "modes": modes, "operations_per_client": args.operations, "rounds": args.rounds,
        "cases": [], "failures": [],
    }
    output = Path(args.output)
    with output.open("x", encoding="utf-8") as file:
        json.dump(report, file, indent=2)
        file.write("\n")

    def save():
        output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")

    cpus = ",".join(map(str, benchmark_cpus))
    for workload in ("disjoint", "shared"):
        for count in clients:
            for mode in modes:
                case = {"workload": workload, "clients": count, "mode": mode, "runs": []}
                report["cases"].append(case)
                for round_index in range(args.rounds):
                    pair = [("baseline", args.baseline_binary), ("candidate", args.candidate_binary)]
                    if round_index % 2:
                        pair.reverse()
                    for label, binary in pair:
                        run = {"revision": label, "round": round_index + 1,
                               "pre_run_quiet": quiet_window(seconds=.20, attempts=3)}
                        case["runs"].append(run)
                        try:
                            before = snapshot()
                            try:
                                command = ["taskset", "-c", cpus, binary, mode, str(count), str(args.operations), workload, args.directory]
                                process = subprocess.run(command, capture_output=True, text=True, timeout=300, check=True)
                                run["measurement"] = parse_result(process.stdout, mode, count, args.operations, workload)
                                run["command"] = command
                            finally:
                                run["noise"] = difference(before, snapshot())
                        except (OSError, ValueError, KeyError, subprocess.SubprocessError) as error:
                            run["error"] = f"{type(error).__name__}: {error}"
                            report["failures"].append({"workload": workload, "clients": count, "mode": mode,
                                                       "round": round_index + 1, "revision": label, "error": run["error"]})
                            save()
                            raise SystemExit("transaction comparison failed; partial report preserved") from error
                        save()
                case["summary"] = {}
                for label in ("baseline", "candidate"):
                    values = [run["measurement"] for run in case["runs"] if run["revision"] == label]
                    case["summary"][label] = {
                        "median_ops_per_second": statistics.median(item["operations_per_second"] for item in values),
                        "median_p50_ns": statistics.median(item["p50_ns"] for item in values),
                        "median_p95_ns": statistics.median(item["p95_ns"] for item in values),
                        "median_p99_ns": statistics.median(item["p99_ns"] for item in values),
                        "median_conflicts": statistics.median(sum(item["conflicts_per_client"]) for item in values),
                    }
                save()
    report["ended_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    save()
    print(f"Completed {sum(len(case['runs']) for case in report['cases'])} verified transaction runs")


if __name__ == "__main__":
    main()
