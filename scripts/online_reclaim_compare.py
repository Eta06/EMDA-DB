#!/usr/bin/env python3
"""Compare disposable retained versus reclaimed online-compaction handoffs."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import statistics
import subprocess
import time

from benchmarks.telemetry import difference, quiet_window, read, snapshot


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def validate(result, mode, keys):
    if (mode not in ("retain", "reclaim") or result.get("mode") != mode or result.get("keys") != keys
            or result.get("live_keys") != keys - keys // 8 or result.get("reopen_verified") is not True
            or result.get("log_format") not in ("V2", "V3") or result.get("staging_clean") is not True):
        raise ValueError("invalid mode, key count or recovery check")
    for name in ("source_log_bytes", "compacted_log_bytes", "previous_log_bytes", "total_log_bytes_after", "elapsed_us"):
        if type(result.get(name)) is not int or result[name] < 0:
            raise ValueError("invalid size or duration")
    previous = result["source_log_bytes"] if mode == "retain" else 0
    if (result["previous_log_bytes"] != previous
            or result["total_log_bytes_after"] != previous + result["compacted_log_bytes"]):
        raise ValueError("unexpected retained log bytes")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", required=True)
    parser.add_argument("--binary", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--keys", default="200,5000")
    parser.add_argument("--rounds", type=int, default=5)
    args = parser.parse_args()
    try:
        counts = [int(value) for value in args.keys.split(",")]
    except ValueError:
        parser.error("keys must be comma-separated integers")
    allowed = sorted(os.sched_getaffinity(0))
    if (not counts or len(set(counts)) != len(counts) or any(not 16 <= n <= 16000 for n in counts)
            or not 2 <= args.rounds <= 50 or not Path(args.directory).is_dir()
            or not Path(args.binary).is_file() or os.path.lexists(args.output) or len(allowed) < 2):
        parser.error("invalid counts, rounds, paths or CPU availability")
    benchmark_cpu, driver_cpu = allowed[0], allowed[-1]
    os.sched_setaffinity(0, {driver_cpu})
    report = {
        "revision": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "git_status": subprocess.check_output(["git", "status", "--short"], text=True),
        "binary_sha256": sha256(args.binary), "script_sha256": sha256(__file__),
        "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "cpu_model": next((line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines() if line.startswith("model name")), "unknown"),
        "cpu_quota": read("/sys/fs/cgroup/cpu.max"), "benchmark_cpu": benchmark_cpu, "driver_cpu": driver_cpu,
        "filesystem": json.loads(subprocess.check_output(["findmnt", "-J", "-T", args.directory, "-o", "TARGET,FSTYPE"], text=True)),
        "scope": "One Linux VM and filesystem; single-client, warm synthetic data, verified online compaction with and without immediate retired-log deletion. Timed work includes validation, copying, sync and cleanup; preparation and reopen checks are outside timers but within noise telemetry. Log file lengths are not allocated-block or total disk measurements. No concurrent-client, physical power-loss, off-host or universal ranking claim.",
        "cases": [], "failures": [],
    }
    output = Path(args.output)
    with output.open("x", encoding="utf-8") as file:
        json.dump(report, file, indent=2)
        file.write("\n")
    for count in counts:
        case = {"keys": count, "runs": []}
        report["cases"].append(case)
        for round_index in range(args.rounds):
            modes = ("retain", "reclaim") if round_index % 2 == 0 else ("reclaim", "retain")
            for mode in modes:
                entry = {"mode": mode, "round": round_index + 1}
                try:
                    entry["pre_run_quiet"] = quiet_window(seconds=.25, attempts=3)
                    before = snapshot()
                    try:
                        command = ["taskset", "-c", str(benchmark_cpu), args.binary, mode, args.directory, str(count)]
                        completed = subprocess.run(command, capture_output=True, text=True, timeout=300)
                        if completed.returncode:
                            raise RuntimeError(f"benchmark exited {completed.returncode}: {completed.stderr[-4096:]}")
                        result = json.loads(completed.stdout)
                        validate(result, mode, count)
                        entry["measurement"] = result
                        entry["command"] = command
                    finally:
                        entry["noise"] = difference(before, snapshot())
                except Exception as error:
                    entry["error"] = f"{type(error).__name__}: {error}"
                    report["failures"].append({"keys": count, "round": round_index + 1, "mode": mode, "error": entry["error"]})
                finally:
                    case["runs"].append(entry)
                    output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        if not report["failures"]:
            case["summary"] = {mode: {"median_us": statistics.median(r["measurement"]["elapsed_us"] for r in case["runs"] if r["mode"] == mode),
                                       "min_us": min(r["measurement"]["elapsed_us"] for r in case["runs"] if r["mode"] == mode),
                                       "max_us": max(r["measurement"]["elapsed_us"] for r in case["runs"] if r["mode"] == mode)} for mode in modes}
        output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    if report["failures"]:
        raise SystemExit("one or more runs failed; see output")
    print(f"Completed {sum(len(case['runs']) for case in report['cases'])} online-compaction runs")


if __name__ == "__main__":
    main()
