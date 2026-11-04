#!/usr/bin/env python3
"""Compare conditional-update latency before and after input admission."""
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


def validate(result, scenario, cycles):
    expected_bytes = {"small": 66, "large": 1048576, "escaped": 1048580, "nested-small": len(json.dumps({"text":"x"*32,"flag":True,"n":7},separators=(",",":"),ensure_ascii=False).encode()), "nested-large": 1048487, "nested-wide": len(json.dumps(list(range(1024)),separators=(",",":")).encode())}
    samples = result.get("samples_ns")
    if (result.get("scenario") != scenario or result.get("cycles") != cycles
            or result.get("verified") is not True or result.get("encoded_bytes") != expected_bytes[scenario]
            or not isinstance(samples, list) or len(samples) != cycles
            or any(type(value) is not int or value < 0 for value in samples)):
        raise ValueError("workload identity, correctness or samples are invalid")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", required=True)
    parser.add_argument("--baseline-binary", required=True)
    parser.add_argument("--baseline-revision", required=True)
    parser.add_argument("--candidate-binary", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--mode", choices=("limit", "counter"), default="limit")
    parser.add_argument("--rounds", type=int, default=4)
    parser.add_argument("--cycles", type=int, default=30)
    args = parser.parse_args()
    cpus = sorted(os.sched_getaffinity(0))
    if (not Path(args.directory).is_dir() or not Path(args.baseline_binary).is_file()
            or not Path(args.candidate_binary).is_file() or os.path.lexists(args.output)
            or not 2 <= args.rounds <= 50 or not 1 <= args.cycles <= 100000 or len(cpus) < 2):
        parser.error("invalid paths, rounds, cycle count or CPU availability")
    benchmark_cpu, driver_cpu = cpus[0], cpus[-1]
    os.sched_setaffinity(0, {driver_cpu})
    scenarios = ("small", "large", "escaped", "nested-small", "nested-large", "nested-wide")
    binaries = {"baseline": args.baseline_binary, "candidate": args.candidate_binary}
    report = {
        "baseline_revision": args.baseline_revision,
        "candidate_revision": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "git_status": subprocess.check_output(["git", "status", "--short"], text=True),
        "binary_sha256": {name: sha(path) for name, path in binaries.items()},
        "script_sha256": sha(__file__),
        "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "platform": platform.platform(), "cpu_quota": read("/sys/fs/cgroup/cpu.max"),
        "cpu_model": next((line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines() if line.startswith("model name")), "unknown"),
        "benchmark_cpu": benchmark_cpu, "driver_cpu": driver_cpu,
        "filesystem": json.loads(subprocess.check_output(["findmnt", "-J", "-T", args.directory, "-o", "TARGET,FSTYPE"], text=True)),
        "mode": args.mode,
        "scope": "One in-process client repeatedly performs a successful set-if-missing update against an existing field, so the supplied value does not change the document. Both revisions execute and synchronize the same document transaction. " + ("The candidate additionally enforces the 4 MiB combined encoded-input budget." if args.mode == "limit" else "Both revisions enforce the 4 MiB combined encoded-input budget; the candidate uses faster exact JSON-value accounting.") + " Inputs are below that limit. Setup and verification are outside the timer; each process has five warmups. The four passes run baseline, candidate, candidate, baseline. This does not measure over-limit rejection, concurrency, physical power loss, off-host restore, total memory, or competitor performance.",
        "rounds": args.rounds, "cycles": args.cycles, "scenarios": scenarios, "runs": [], "failures": [],
    }
    output = Path(args.output)
    with output.open("x", encoding="utf-8") as file:
        json.dump(report, file, indent=2)
        file.write("\n")

    def save():
        output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")

    for pass_index, variant in enumerate(("baseline", "candidate", "candidate", "baseline")):
        for round_index in range(args.rounds):
            rotation = (round_index + pass_index) % len(scenarios)
            for scenario in scenarios[rotation:] + scenarios[:rotation]:
                entry = {"pass": pass_index + 1, "variant": variant, "round": round_index + 1, "scenario": scenario}
                try:
                    entry["pre_run_quiet"] = quiet_window(seconds=.25, attempts=3)
                    before = snapshot()
                    try:
                        command = ["taskset", "-c", str(benchmark_cpu), binaries[variant], args.directory, scenario, str(args.cycles)]
                        completed = subprocess.run(command, capture_output=True, text=True, timeout=300)
                        if completed.returncode:
                            raise RuntimeError(f"benchmark exited {completed.returncode}: {completed.stderr[-4096:]}")
                        result = json.loads(completed.stdout)
                        validate(result, scenario, args.cycles)
                        entry["measurement"] = result
                        entry["command"] = command
                    finally:
                        entry["noise"] = difference(before, snapshot())
                except Exception as error:
                    entry["error"] = f"{type(error).__name__}: {error}"
                    report["failures"].append({"pass": pass_index + 1, "round": round_index + 1, "scenario": scenario, "error": entry["error"]})
                finally:
                    report["runs"].append(entry)
                    save()
    if report["failures"]:
        raise SystemExit("one or more comparison runs failed; see output")
    report["summary"] = []
    for scenario in scenarios:
        variants = {}
        for variant in binaries:
            medians = [statistics.median(run["measurement"]["samples_ns"]) / 1000 for run in report["runs"] if run["scenario"] == scenario and run["variant"] == variant]
            variants[variant] = {"median_of_round_medians_us": statistics.median(medians), "round_median_min_us": min(medians), "round_median_max_us": max(medians)}
        report["summary"].append({"scenario": scenario, "variants": variants})
    save()
    print(f"Completed {len(report['runs'])} verified paired comparison runs")


if __name__ == "__main__":
    main()
