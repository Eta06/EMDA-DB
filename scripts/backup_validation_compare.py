#!/usr/bin/env python3
"""Compare restore validation before and after streaming, with checked output."""
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
    parser.add_argument("--baseline-binary", required=True)
    parser.add_argument("--baseline-revision", required=True)
    parser.add_argument("--binary", default="target/release/examples/backup_validation_bench")
    parser.add_argument("--prepare-binary", default="target/release/examples/index_memory_bench")
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
            or not Path(args.directory).is_dir() or any(not Path(path).is_file() for path in (args.binary, args.baseline_binary, args.prepare_binary))
            or os.path.lexists(args.output)):
        parser.error("invalid counts, options, binaries, fixture directory or output path")
    allowed = sorted(os.sched_getaffinity(0))
    if len(allowed) < 2:
        parser.error("at least two allowed CPUs are required")
    benchmark_cpu, driver_cpu = allowed[0], allowed[-1]
    os.sched_setaffinity(0, {driver_cpu})
    sources = sorted([*Path("src").rglob("*.rs"), Path("Cargo.toml"), Path("Cargo.lock"),
                      Path("examples/index_memory_bench.rs"), Path("examples/backup_validation_bench.rs"),
                      Path("scripts/benchmarks/telemetry.py"), Path(__file__)])
    baseline_revision = subprocess.check_output(["git", "rev-parse", "--verify", args.baseline_revision + "^{commit}"], text=True).strip()
    report = {
        "revision": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "baseline_revision": baseline_revision,
        "git_status": subprocess.check_output(["git", "status", "--short"], text=True),
        "source_sha256": {str(path): sha256(path) for path in sources},
        "binary_sha256": sha256(args.binary), "baseline_binary_sha256": sha256(args.baseline_binary),
        "prepare_binary_sha256": sha256(args.prepare_binary),
        "binary_provenance": "Baseline revision is caller-supplied. Build against that source with the same benchmark example copied in; binary and example hashes are recorded.",
        "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "cpu_model": next((line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines() if line.startswith("model name")), "unknown"),
        "cpu_quota": read("/sys/fs/cgroup/cpu.max"), "allowed_cpus": allowed,
        "benchmark_cpu": benchmark_cpu, "driver_cpu": driver_cpu,
        "filesystem": json.loads(subprocess.check_output(["findmnt", "-J", "-T", args.directory, "-o", "TARGET,FSTYPE"], text=True)),
        "limitations": "One Linux VM, one filesystem, warm cache and fresh processes. Both restore the same verified backup; timing includes validation, copying, synchronization and publication, but excludes later log hashing, reopening and sampled value checks. Peak RSS is read before reopening and excludes OS page cache. VM/cgroup noise includes setup and verification. A local restore is not an off-host drill or physical power-loss test; this is not a competitor ranking.",
        "cases": [],
    }
    with tempfile.TemporaryDirectory(prefix=".backup-validation-bench-", dir=args.directory) as root:
        for count in counts:
            parent = Path(root) / str(count)
            parent.mkdir()
            base = ["taskset", "-c", str(benchmark_cpu)]
            prepared = subprocess.run(base + [args.prepare_binary, "prepare", str(parent), str(count),
                                      str(args.key_bytes), str(args.tail_keys)], check=True, capture_output=True, text=True, timeout=300)
            info = fields(prepared.stdout)
            if int(info["keys"]) != count or int(info["tail_keys"]) != args.tail_keys:
                raise RuntimeError("unexpected fixture")
            setup = subprocess.run(base + [args.binary, "make-backup", str(parent), info["token"]], check=True, capture_output=True, text=True, timeout=300)
            if fields(setup.stdout).get("prepared") != "true":
                raise RuntimeError("backup setup failed")
            case = {"keys": count, "key_bytes": args.key_bytes, "tail_keys": args.tail_keys,
                    "log_bytes": int(info["log_bytes"]), "backup_bytes": (parent / "backup").stat().st_size,
                    "quiet_check": quiet_window(), "runs": []}
            reference_digest = None
            for round_index in range(args.rounds):
                modes = (("baseline", args.baseline_binary), ("streaming", args.binary))
                if round_index % 2:
                    modes = tuple(reversed(modes))
                for mode, binary in modes:
                    before = snapshot()
                    command = base + [binary, "restore", str(parent), str(count), str(args.key_bytes), str(args.tail_keys)]
                    process = subprocess.run(command, check=True, capture_output=True, text=True, timeout=300)
                    noise = difference(before, snapshot())
                    sample = fields(process.stdout)
                    if reference_digest is None:
                        reference_digest = sample["digest"]
                    if len(sample["digest"]) != 64 or sample["digest"] != reference_digest:
                        raise RuntimeError("restored log digests differ")
                    values = {key: int(sample[key]) for key in ("elapsed_us", "before_rss_kib", "after_rss_kib", "peak_rss_kib")}
                    if values["peak_rss_kib"] < max(values["before_rss_kib"], values["after_rss_kib"]):
                        raise RuntimeError("invalid process RSS sample")
                    case["runs"].append({"round": round_index + 1, "mode": mode, **values, "noise": noise})
            case["restored_log_digest"] = reference_digest
            case["summary"] = {}
            for mode in ("baseline", "streaming"):
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
