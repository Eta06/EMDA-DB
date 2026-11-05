#!/usr/bin/env python3
"""Paired read-admission overhead comparison using the checked KV workload."""
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


def fields(line):
    return dict(item.split("=", 1) for item in line.split() if "=" in item)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--directory", required=True)
    p.add_argument("--before", required=True)
    p.add_argument("--after", required=True)
    p.add_argument("--baseline-revision", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--rounds", type=int, default=4)
    p.add_argument("--operations", type=int, default=2000)
    p.add_argument("--include-limited", action="store_true", help="also measure the candidate with read admission enabled")
    a = p.parse_args()
    allowed = sorted(os.sched_getaffinity(0))
    if (not Path(a.directory).is_dir() or not Path(a.before).is_file() or not Path(a.after).is_file()
            or os.path.lexists(a.output) or len(allowed) < 2 or not 2 <= a.rounds <= 50
            or not 100 <= a.operations <= 100000):
        p.error("invalid paths, CPU availability, rounds or operation count")
    baseline = subprocess.check_output(["git", "rev-parse", "--verify", a.baseline_revision + "^{commit}"], text=True).strip()
    workers, driver = allowed[:min(4, len(allowed) - 1)], allowed[-1]
    os.sched_setaffinity(0, {driver})
    cpus = ",".join(map(str, workers))
    report = {
        "revision": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "git_status": subprocess.check_output(["git", "status", "--short"], text=True),
        "baseline_revision": baseline, "baseline_provenance": "Caller-supplied revision; independently built binary hash is retained.",
        "before_sha256": sha(a.before), "after_sha256": sha(a.after), "script_sha256": sha(__file__),
        "example_sha256": sha("examples/kv_bench.rs"), "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "platform": platform.platform(), "python": sys.version, "cpu_quota": read("/sys/fs/cgroup/cpu.max"),
        "cpu_model": next((x.split(":", 1)[1].strip() for x in Path("/proc/cpuinfo").read_text().splitlines() if x.startswith("model name")), "unknown"),
        "benchmark_cpus": workers, "driver_cpu": driver,
        "filesystem": json.loads(subprocess.check_output(["findmnt", "-J", "-T", a.directory, "-o", "TARGET,FSTYPE"], text=True)),
        "scope": "Rotated fresh processes, one or four closed-loop clients, 16 existing keys and 64-byte values, read-only or 50% writes. Candidate default admission is disabled; when requested a separate variant enables the maximum admission limit. Writes synchronize before acknowledgement in all versions; returned data and sampled reopen values are checked. Results contain per-run latency summaries, not individual samples. Setup/reopen are outside timed operations but within telemetry. No checkpoint-store, saturation, total-memory, physical-power-loss or off-host comparison.",
        "include_limited": a.include_limited,
        "rounds": a.rounds, "operations_per_client": a.operations, "runs": [], "failures": []}
    output = Path(a.output)
    with output.open("x", encoding="utf-8") as stream:
        json.dump(report, stream, indent=2)
        stream.write("\n")
    for round_index in range(a.rounds):
        cases = [(1, 0), (1, 50), (4, 0), (4, 50)]
        rotation = round_index % len(cases)
        for index, (clients, write_pct) in enumerate(cases[rotation:] + cases[:rotation]):
            variants = [("before", a.before, "prototype"), ("after", a.after, "prototype")]
            if a.include_limited:
                variants.append(("after_limited", a.after, "prototype-read-limited"))
            order = (round_index + index) % len(variants)
            variants = variants[order:] + variants[:order]
            for variant, binary, backend in variants:
                entry = {"variant": variant, "clients": clients, "write_percent": write_pct, "round": round_index + 1}
                try:
                    entry["pre_run_quiet"] = quiet_window(seconds=.25, attempts=3)
                    before = snapshot()
                    try:
                        command = ["taskset", "-c", cpus, binary, backend, str(clients), str(a.operations), "16", str(write_pct), a.directory, "64"]
                        proc = subprocess.run(command, capture_output=True, text=True, timeout=300, check=True)
                        lines = proc.stdout.strip().splitlines()
                        head = fields(lines[0])
                        latency = {line.split(":", 1)[0]: fields(line) for line in lines if line.startswith(("read:", "write:"))}
                        if (head.get("backend") != backend or int(head.get("completed", 0)) != clients * a.operations
                                or "verified_reopen=true" not in lines or int(latency["read"]["count"]) == 0):
                            raise ValueError("benchmark identity, reads or recovery check failed")
                        if write_pct == 0 and int(latency["write"]["count"]) != 0:
                            raise ValueError("read-only workload wrote data")
                        if write_pct and int(latency["write"]["count"]) == 0:
                            raise ValueError("mixed workload contained no writes")
                        entry.update(command=command, stdout=proc.stdout, latency=latency, operations_per_second=float(head["ops_per_sec"]))
                    finally:
                        entry["noise"] = difference(before, snapshot())
                except Exception as error:
                    entry["error"] = f"{type(error).__name__}: {error}"
                    report["failures"].append({**{k: entry[k] for k in ("variant", "clients", "write_percent", "round")}, "error": entry["error"]})
                finally:
                    report["runs"].append(entry)
                    output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    if report["failures"]:
        raise SystemExit("one or more runs failed; see output")
    print(f"Completed {len(report['runs'])} checked read-admission runs")


if __name__ == "__main__":
    main()
