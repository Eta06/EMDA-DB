#!/usr/bin/env python3
"""Compare verified checkpoint creation and live rollover on one Linux VM."""
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", required=True, help="existing directory for disposable fixtures")
    parser.add_argument("--output", required=True, help="new JSON output path")
    parser.add_argument("--prepare-binary", default="target/release/examples/index_memory_bench")
    parser.add_argument("--binary", default="target/release/examples/checkpoint_rollover_bench")
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--keys", default="10000,200000")
    parser.add_argument("--key-bytes", type=int, default=128)
    parser.add_argument("--tail-keys", type=int, default=4096)
    args = parser.parse_args()
    try:
        counts = [int(item) for item in args.keys.split(",")]
    except ValueError:
        parser.error("keys must be comma-separated integers")
    if (not counts or len(set(counts)) != len(counts) or any(not 1 <= value <= 1_000_000 for value in counts)
            or not 2 <= args.rounds <= 50 or not 8 <= args.key_bytes <= 1024 or not 0 <= args.tail_keys <= 4096
            or not Path(args.directory).is_dir() or not Path(args.binary).is_file()
            or not Path(args.prepare_binary).is_file() or os.path.lexists(args.output)):
        parser.error("invalid counts, options, binaries, fixture directory or output path")
    allowed = sorted(os.sched_getaffinity(0))
    if len(allowed) < 2:
        parser.error("at least two allowed CPUs are required")
    benchmark_cpu, driver_cpu = allowed[0], allowed[-1]
    os.sched_setaffinity(0, {driver_cpu})
    sources = sorted([*Path("src").rglob("*.rs"), Path("Cargo.toml"), Path("Cargo.lock"),
                      Path("examples/index_memory_bench.rs"), Path("examples/checkpoint_rollover_bench.rs"),
                      Path("scripts/benchmarks/telemetry.py"), Path(__file__)])
    report = {
        "revision": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "git_status": subprocess.check_output(["git", "status", "--short"], text=True),
        "source_sha256": {str(path): sha256(path) for path in sources},
        "binary_sha256": sha256(args.binary), "prepare_binary_sha256": sha256(args.prepare_binary),
        "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "cpu_model": next((line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines() if line.startswith("model name")), "unknown"),
        "cpu_quota": read("/sys/fs/cgroup/cpu.max"), "allowed_cpus": allowed,
        "benchmark_cpu": benchmark_cpu, "driver_cpu": driver_cpu,
        "filesystem": json.loads(subprocess.check_output(["findmnt", "-J", "-T", args.directory, "-o", "TARGET,FSTYPE"], text=True)),
        "limitations": "One Linux VM, one filesystem, warm cache and separate fresh processes. Ordinary Store creates a checkpoint after full-index recovery; CheckpointStore creates and switches to its streamed checkpoint. The timer includes each method's validation and synchronization, including switching only for CheckpointStore; opening and sampled value checks are outside the timer. Each checkpoint must have the same SHA-256 digest and is reopened and checked. RSS and peak RSS exclude OS page cache and do not represent total project memory. Noise counters include fixture setup and verification. This is not a competitor or production ranking.",
        "cases": [],
    }
    with tempfile.TemporaryDirectory(prefix=".checkpoint-rollover-bench-", dir=args.directory) as root:
        for count in counts:
            parent = Path(root) / str(count)
            parent.mkdir()
            prepare = ["taskset", "-c", str(benchmark_cpu), args.prepare_binary, "prepare", str(parent),
                       str(count), str(args.key_bytes), str(args.tail_keys)]
            info = fields(subprocess.run(prepare, check=True, capture_output=True, text=True, timeout=300).stdout)
            if int(info["keys"]) != count or int(info["tail_keys"]) != args.tail_keys:
                raise RuntimeError("unexpected fixture")
            case = {"keys": count, "key_bytes": args.key_bytes, "tail_keys": args.tail_keys,
                    "tail_bytes": int(info["tail_bytes"]), "log_bytes": int(info["log_bytes"]),
                    "quiet_check": quiet_window(), "runs": []}
            reference_digest = None
            for round_index in range(args.rounds):
                modes = ("store", "checkpoint") if round_index % 2 == 0 else ("checkpoint", "store")
                for mode in modes:
                    before = snapshot()
                    command = ["taskset", "-c", str(benchmark_cpu), args.binary, str(parent), str(count),
                               str(args.key_bytes), str(args.tail_keys), mode, info["token"]]
                    measured = subprocess.run(command, check=True, capture_output=True, text=True, timeout=300)
                    noise = difference(before, snapshot())
                    sample = fields(measured.stdout)
                    if sample["mode"] != mode or len(sample["digest"]) != 64:
                        raise RuntimeError("unexpected benchmark output")
                    if reference_digest is None:
                        reference_digest = sample["digest"]
                    if sample["digest"] != reference_digest:
                        raise RuntimeError("generated checkpoint digests differ")
                    values = {key: int(sample[key]) for key in ("elapsed_us", "before_rss_kib", "after_rss_kib", "peak_rss_kib", "checkpoint_bytes")}
                    if values["peak_rss_kib"] < max(values["before_rss_kib"], values["after_rss_kib"]):
                        raise RuntimeError("invalid process RSS sample")
                    case["runs"].append({"round": round_index + 1, "mode": mode, **values, "noise": noise})
            case["checkpoint_digest"] = reference_digest
            case["summary"] = {}
            for mode in ("store", "checkpoint"):
                samples = [item for item in case["runs"] if item["mode"] == mode]
                case["summary"][mode] = {"median_" + key: statistics.median(item[key] for item in samples)
                                         for key in ("elapsed_us", "before_rss_kib", "after_rss_kib", "peak_rss_kib")}
            report["cases"].append(case)
    with open(args.output, "x", encoding="utf-8") as output:
        json.dump(report, output, indent=2)
        output.write("\n")
    for case in report["cases"]:
        print(f"keys={case['keys']} summary={case['summary']}")


if __name__ == "__main__":
    main()
