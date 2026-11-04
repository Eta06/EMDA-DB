#!/usr/bin/env python3
"""Measure warm checkpoint opens, sampled RSS and checked point reads on Linux."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import statistics
import subprocess
import tempfile
import time

from benchmarks.telemetry import difference, quiet_window, read, snapshot


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def fields(output):
    return dict(part.split("=", 1) for part in output.strip().split())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", required=True, help="existing directory for disposable fixtures")
    parser.add_argument("--output", required=True, help="new JSON result file")
    parser.add_argument("--binary", default="target/release/examples/index_memory_bench")
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--keys", default="10000,200000")
    parser.add_argument("--key-bytes", type=int, default=128)
    parser.add_argument("--tail-keys", type=int, default=0, help="distinct keys added after the checkpoint (0–4096)")
    args = parser.parse_args()
    try:
        counts = [int(item) for item in args.keys.split(",")]
    except ValueError:
        parser.error("keys must be comma-separated integers")
    if not counts or len(set(counts)) != len(counts) or any(not 1 <= count <= 1_000_000 for count in counts):
        parser.error("key counts must be distinct and between 1 and 1000000")
    if not 2 <= args.rounds <= 50 or not 8 <= args.key_bytes <= 1024 or not 0 <= args.tail_keys <= 4096:
        parser.error("rounds, key size or tail key count out of range")
    if not Path(args.directory).is_dir() or not Path(args.binary).is_file():
        parser.error("an existing fixture directory and prebuilt binary are required")
    if os.path.lexists(args.output):
        parser.error("output must not already exist")
    allowed = sorted(os.sched_getaffinity(0))
    if len(allowed) < 2:
        parser.error("at least two CPUs in the allowed affinity mask are required")
    bench_cpu, driver_cpu = allowed[0], allowed[-1]
    os.sched_setaffinity(0, {driver_cpu})
    sources = sorted([*Path("src").rglob("*.rs"), Path("Cargo.toml"), Path("Cargo.lock"),
                      Path("examples/index_memory_bench.rs"), Path("scripts/benchmarks/telemetry.py"), Path(__file__)])
    result = {
        "revision": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "git_status": subprocess.check_output(["git", "status", "--short"], text=True),
        "source_sha256": {str(path): digest(path) for path in sources},
        "binary_sha256": digest(args.binary),
        "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "cpu_model": next((line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines() if line.startswith("model name")), "unknown"),
        "cpu_quota": read("/sys/fs/cgroup/cpu.max"), "allowed_cpus": allowed,
        "benchmark_cpu": bench_cpu, "driver_cpu": driver_cpu,
        "filesystem": json.loads(subprocess.check_output(["findmnt", "-J", "-T", args.directory, "-o", "TARGET,FSTYPE"], text=True)),
        "limitations": "One Linux VM, warm cache, short direct in-process reads in separate fresh processes. The historical view does not replay or validate the tail; the recovered view replays a bounded witnessed tail. Both are read-only and hold a shared lock excluding writers; the store is writable. Each opening hashes the checkpoint-covered log prefix. RSS is Linux process RSS and excludes OS page cache; it is not a memory quota. VM/cgroup noise includes the workload. This is not a production or competitor comparison.",
        "cases": [],
    }
    with tempfile.TemporaryDirectory(prefix=".index-memory-bench-", dir=args.directory) as root:
        for count in counts:
            parent = Path(root) / str(count)
            parent.mkdir()
            base = ["taskset", "-c", str(bench_cpu), args.binary]
            prepared = subprocess.run(base + ["prepare", str(parent), str(count), str(args.key_bytes), str(args.tail_keys)], check=True, text=True, capture_output=True, timeout=300)
            info = fields(prepared.stdout)
            if int(info["keys"]) != count or int(info["key_bytes"]) != args.key_bytes or int(info["tail_keys"]) != args.tail_keys:
                raise RuntimeError("unexpected fixture configuration")
            case = {"keys": count, "key_bytes": args.key_bytes,
                    "tail_keys": args.tail_keys, "tail_bytes": int(info["tail_bytes"]),
                    "log_bytes": int(info["log_bytes"]), "checkpoint_bytes": int(info["checkpoint_bytes"]),
                    "quiet_check": quiet_window(), "runs": []}
            modes = ("store", "view", "recovered")
            for round_index in range(args.rounds):
                offset = round_index % len(modes)
                for mode in modes[offset:] + modes[:offset]:
                    before = snapshot()
                    command = base + ["measure", str(parent), str(count), str(args.key_bytes), mode, info["token"], str(args.tail_keys)]
                    measured = subprocess.run(command, check=True, text=True, capture_output=True, timeout=120)
                    noise = difference(before, snapshot())
                    sample = fields(measured.stdout)
                    if sample["mode"] != mode:
                        raise RuntimeError("unexpected measurement mode")
                    lookups = [int(value) for value in sample["lookups_ns"].split(",")]
                    if len(lookups) != 16:
                        raise RuntimeError("incomplete lookup sample")
                    tail_lookups = [] if sample["tail_lookups_ns"] == "none" else [int(value) for value in sample["tail_lookups_ns"].split(",")]
                    expected_tail_samples = 16 if args.tail_keys and mode != "view" else 0
                    if len(tail_lookups) != expected_tail_samples or int(sample["overlay_keys"]) != (args.tail_keys if mode == "recovered" else 0):
                        raise RuntimeError("incomplete tail sample or unexpected overlay size")
                    case["runs"].append({"round": round_index + 1, "mode": mode,
                        "open_us": int(sample["open_us"]), "rss_kib": int(sample["rss_kib"]),
                        "after_rss_kib": int(sample["after_rss_kib"]), "blocks": int(sample["blocks"]),
                        "overlay_keys": int(sample["overlay_keys"]), "lookups_ns": lookups,
                        "tail_lookups_ns": tail_lookups, "noise": noise})
            case["summary"] = {}
            for mode in modes:
                samples = [run for run in case["runs"] if run["mode"] == mode]
                case["summary"][mode] = {
                    "median_open_us": statistics.median(run["open_us"] for run in samples),
                    "median_rss_kib": statistics.median(run["rss_kib"] for run in samples),
                    "median_after_rss_kib": statistics.median(run["after_rss_kib"] for run in samples),
                    "median_lookup_ns": statistics.median(value for run in samples for value in run["lookups_ns"]),
                }
                if args.tail_keys and mode != "view":
                    case["summary"][mode]["median_tail_lookup_ns"] = statistics.median(value for run in samples for value in run["tail_lookups_ns"])
            result["cases"].append(case)
    with open(args.output, "x", encoding="utf-8") as output:
        json.dump(result, output, indent=2)
        output.write("\n")
    for case in result["cases"]:
        print(f"keys={case['keys']} summary={case['summary']}")


if __name__ == "__main__":
    main()
