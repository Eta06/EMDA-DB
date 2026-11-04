#!/usr/bin/env python3
"""Compare ordinary and checkpoint-backed separate-copy compaction."""
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
    parser.add_argument("--directory", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--binary", default="target/release/examples/checkpoint_compaction_bench")
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--keys", default="10000,50000")
    parser.add_argument("--key-bytes", type=int, default=128)
    parser.add_argument("--tail-keys", type=int, default=4096)
    args = parser.parse_args()
    try:
        counts = [int(item) for item in args.keys.split(",")]
    except ValueError:
        parser.error("keys must be comma-separated integers")
    if (not counts or len(set(counts)) != len(counts) or any(not 1 <= value <= 1_000_000 for value in counts)
            or not 2 <= args.rounds <= 50 or not 8 <= args.key_bytes <= 1024 or not 0 <= args.tail_keys <= 4096
            or not Path(args.directory).is_dir() or not Path(args.binary).is_file() or os.path.lexists(args.output)):
        parser.error("invalid counts, options, binary, fixture directory or output path")
    allowed = sorted(os.sched_getaffinity(0))
    if len(allowed) < 2:
        parser.error("at least two allowed CPUs are required")
    benchmark_cpu, driver_cpu = allowed[0], allowed[-1]
    os.sched_setaffinity(0, {driver_cpu})
    sources = sorted([*Path("src").rglob("*.rs"), Path("Cargo.toml"), Path("Cargo.lock"),
                      Path("examples/checkpoint_compaction_bench.rs"), Path("scripts/benchmarks/telemetry.py"), Path(__file__)])
    report = {
        "revision": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "git_status": subprocess.check_output(["git", "status", "--short"], text=True),
        "source_sha256": {str(path): sha256(path) for path in sources}, "binary_sha256": sha256(args.binary),
        "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "cpu_model": next((line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines() if line.startswith("model name")), "unknown"),
        "cpu_quota": read("/sys/fs/cgroup/cpu.max"), "allowed_cpus": allowed,
        "benchmark_cpu": benchmark_cpu, "driver_cpu": driver_cpu,
        "filesystem": json.loads(subprocess.check_output(["findmnt", "-J", "-T", args.directory, "-o", "TARGET,FSTYPE"], text=True)),
        "limitations": "One Linux VM and filesystem, warm cache, separate fresh processes and direct APIs. Both compact the same source; the checkpoint-backed path additionally creates and verifies a checkpoint, while the ordinary path does not. Timers include validation, copying, synchronization and publication; opening and later sample checks are excluded. Compacted record bytes after the unique log header must match. Peak RSS excludes OS page cache. Noise includes setup and verification. Neither path is online compaction or a total-memory guarantee; this is not a competitor ranking or power-loss test.",
        "cases": [],
    }
    with tempfile.TemporaryDirectory(prefix=".checkpoint-compaction-bench-", dir=args.directory) as root:
        for count in counts:
            parent = Path(root) / str(count)
            parent.mkdir()
            base = ["taskset", "-c", str(benchmark_cpu), args.binary]
            prepared = subprocess.run(base + ["prepare", str(parent), str(count), str(args.key_bytes), str(args.tail_keys)], check=True, capture_output=True, text=True, timeout=300)
            info = fields(prepared.stdout)
            tail = min(count, args.tail_keys)
            if int(info["keys"]) != count or int(info["tail_keys"]) != tail:
                raise RuntimeError("unexpected fixture")
            case = {"keys": count, "key_bytes": args.key_bytes, "tail_keys": tail,
                    "source_bytes": int(info["source_bytes"]), "quiet_check": quiet_window(), "runs": []}
            reference_digest = None
            target_bytes = None
            for round_index in range(args.rounds):
                modes = ("store", "checkpoint") if round_index % 2 == 0 else ("checkpoint", "store")
                for mode in modes:
                    before = snapshot()
                    command = base + ["measure", str(parent), str(count), str(args.key_bytes), str(tail), mode, info["token"]]
                    process = subprocess.run(command, check=True, capture_output=True, text=True, timeout=300)
                    noise = difference(before, snapshot())
                    sample = fields(process.stdout)
                    if sample["mode"] != mode or len(sample["record_digest"]) != 64:
                        raise RuntimeError("unexpected benchmark output")
                    if reference_digest is None:
                        reference_digest, target_bytes = sample["record_digest"], int(sample["target_bytes"])
                    if sample["record_digest"] != reference_digest or int(sample["target_bytes"]) != target_bytes:
                        raise RuntimeError("compacted record bytes differ")
                    values = {key: int(sample[key]) for key in ("elapsed_us", "before_rss_kib", "after_rss_kib", "peak_rss_kib", "target_bytes", "checkpoint_bytes")}
                    if values["peak_rss_kib"] < max(values["before_rss_kib"], values["after_rss_kib"]):
                        raise RuntimeError("invalid process RSS sample")
                    case["runs"].append({"round": round_index + 1, "mode": mode, **values, "noise": noise})
            case["record_digest"] = reference_digest
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
