#!/usr/bin/env python3
"""Compare bounded checkpoint and ordinary snapshot creation on Linux."""
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


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def fields(output):
    return dict(part.split("=", 1) for part in output.strip().split())


def run(command):
    result = subprocess.run(command, capture_output=True, text=True, timeout=300)
    if result.returncode:
        raise RuntimeError(f"benchmark failed ({result.returncode}): {result.stderr.strip()}")
    return fields(result.stdout)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--prepare-binary", default="target/release/examples/index_memory_bench")
    parser.add_argument("--binary", default="target/release/examples/checkpoint_snapshot_bench")
    parser.add_argument("--keys", default="12000,200000")
    parser.add_argument("--key-bytes", type=int, default=128)
    parser.add_argument("--tail-keys", type=int, default=4096)
    parser.add_argument("--snapshots", type=int, default=4)
    parser.add_argument("--rounds", type=int, default=5)
    args = parser.parse_args()
    try:
        counts = [int(item) for item in args.keys.split(",")]
    except ValueError:
        parser.error("keys must be comma-separated integers")
    if (not counts or len(set(counts)) != len(counts) or any(not 1 <= count <= 1_000_000 for count in counts)
            or not 8 <= args.key_bytes <= 1024 or not 0 <= args.tail_keys <= 4096
            or not 1 <= args.snapshots <= 16 or not 2 <= args.rounds <= 50
            or args.tail_keys * args.snapshots > 65536
            or args.tail_keys * args.key_bytes * args.snapshots > 16 * 1024 * 1024):
        parser.error("invalid keys, key size, tail keys, snapshots or rounds")
    if (not Path(args.directory).is_dir() or not Path(args.prepare_binary).is_file()
            or not Path(args.binary).is_file() or os.path.lexists(args.output)):
        parser.error("existing directory and binaries, and a new output path, are required")
    allowed = sorted(os.sched_getaffinity(0))
    if len(allowed) < 2:
        parser.error("at least two allowed CPUs are required")
    benchmark_cpu, driver_cpu = allowed[0], allowed[-1]
    os.sched_setaffinity(0, {driver_cpu})
    sources = sorted([*Path("src").rglob("*.rs"), Path("Cargo.toml"), Path("Cargo.lock"),
                      Path("examples/index_memory_bench.rs"), Path("examples/checkpoint_snapshot_bench.rs"),
                      Path("scripts/benchmarks/telemetry.py"), Path(__file__)])
    report = {
        "revision": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "git_status": subprocess.check_output(["git", "status", "--short"], text=True),
        "source_sha256": {str(path): sha256(path) for path in sources},
        "prepare_binary_sha256": sha256(args.prepare_binary), "binary_sha256": sha256(args.binary),
        "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "cpu_model": next((line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines() if line.startswith("model name")), "unknown"),
        "cpu_quota": read("/sys/fs/cgroup/cpu.max"), "allowed_cpus": allowed,
        "benchmark_cpu": benchmark_cpu, "driver_cpu": driver_cpu,
        "filesystem": json.loads(subprocess.check_output(["findmnt", "-J", "-T", args.directory, "-o", "TARGET,FSTYPE"], text=True)),
        "limitations": "One Linux VM, warm cache and fresh processes sharing a prepared fixture. Ordinary snapshots copy the complete bounded in-memory index; checkpoint snapshots copy the checkpoint block directory and bounded suffix overlay. Opening hashes the covered log prefix. Linux process RSS and high-water RSS exclude OS page cache and are not total-memory guarantees. Snapshot creation timings exclude opening and fixture preparation. No concurrent mutation, physical power loss or general performance ranking is measured. VM/cgroup noise includes the full process.",
        "cases": [],
    }
    with tempfile.TemporaryDirectory(prefix=".checkpoint-snapshot-bench-", dir=args.directory) as root:
        for count in counts:
            parent = Path(root) / str(count)
            parent.mkdir()
            base = ["taskset", "-c", str(benchmark_cpu)]
            prepared = run(base + [args.prepare_binary, "prepare", str(parent), str(count), str(args.key_bytes), str(args.tail_keys)])
            if (int(prepared["keys"]) != count or int(prepared["key_bytes"]) != args.key_bytes
                    or int(prepared["tail_keys"]) != args.tail_keys):
                raise RuntimeError("unexpected fixture configuration")
            live = count + args.tail_keys
            ordinary_supported = (live <= 16384 and live * args.key_bytes <= 4 * 1024 * 1024
                                  and live * args.snapshots <= 65536
                                  and live * args.key_bytes * args.snapshots <= 16 * 1024 * 1024)
            modes = ("ordinary", "checkpoint") if ordinary_supported else ("checkpoint",)
            case = {"base_keys": count, "tail_keys": args.tail_keys, "key_bytes": args.key_bytes,
                    "snapshots": args.snapshots, "ordinary_supported": ordinary_supported,
                    "log_bytes": int(prepared["log_bytes"]), "checkpoint_bytes": int(prepared["checkpoint_bytes"]),
                    "quiet_check": quiet_window(), "runs": []}
            for index in range(args.rounds):
                order = modes[index % len(modes):] + modes[:index % len(modes)]
                for mode in order:
                    before = snapshot()
                    values = run(base + [args.binary, str(parent), str(count), str(args.key_bytes),
                                         str(args.tail_keys), str(args.snapshots), mode, prepared["token"]])
                    noise = difference(before, snapshot())
                    if (values["mode"] != mode or int(values["base_keys"]) != count
                            or int(values["tail_keys"]) != args.tail_keys or int(values["snapshots"]) != args.snapshots):
                        raise RuntimeError("unexpected measurement configuration")
                    durations = [int(value) for value in values["snapshot_ns"].split(",")]
                    if len(durations) != args.snapshots:
                        raise RuntimeError("incomplete snapshot samples")
                    before_rss, after_rss = int(values["before_rss_kib"]), int(values["after_rss_kib"])
                    case["runs"].append({"round": index + 1, "mode": mode, "open_us": int(values["open_us"]),
                        "before_rss_kib": before_rss, "after_rss_kib": after_rss,
                        "rss_delta_kib": after_rss - before_rss, "peak_rss_kib": int(values["peak_rss_kib"]),
                        "snapshot_ns": durations, "noise": noise})
            case["summary"] = {}
            for mode in modes:
                runs = [run for run in case["runs"] if run["mode"] == mode]
                case["summary"][mode] = {
                    "median_open_us": statistics.median(run["open_us"] for run in runs),
                    "median_before_rss_kib": statistics.median(run["before_rss_kib"] for run in runs),
                    "median_after_rss_kib": statistics.median(run["after_rss_kib"] for run in runs),
                    "median_rss_delta_kib": statistics.median(run["rss_delta_kib"] for run in runs),
                    "median_peak_rss_kib": statistics.median(run["peak_rss_kib"] for run in runs),
                    "median_snapshot_ns": statistics.median(value for run in runs for value in run["snapshot_ns"]),
                }
            report["cases"].append(case)
    with open(args.output, "x", encoding="utf-8") as output:
        json.dump(report, output, indent=2)
        output.write("\n")
    for case in report["cases"]:
        print(f"base_keys={case['base_keys']} summary={case['summary']}")


if __name__ == "__main__":
    main()
