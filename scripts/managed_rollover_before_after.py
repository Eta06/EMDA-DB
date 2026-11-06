#!/usr/bin/env python3
"""Compare managed checkpoint rollover across two release binaries on one fixture."""
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
    parser.add_argument("--binary", required=True)
    parser.add_argument("--prepare-binary", required=True)
    parser.add_argument("--rounds", type=int, default=10)
    parser.add_argument("--keys", default="100,1000")
    parser.add_argument("--key-bytes", type=int, default=32)
    parser.add_argument("--tail-keys", type=int, default=64)
    args = parser.parse_args()
    try:
        counts = [int(item) for item in args.keys.split(",")]
    except ValueError:
        parser.error("keys must be comma-separated integers")
    if (
        not counts or len(set(counts)) != len(counts)
        or any(not 1 <= count <= 1_000_000 for count in counts)
        or not 2 <= args.rounds <= 50 or not 8 <= args.key_bytes <= 1024
        or not 0 <= args.tail_keys <= 4096 or not Path(args.directory).is_dir()
        or any(not Path(path).is_file() for path in (args.binary, args.baseline_binary, args.prepare_binary))
        or os.path.lexists(args.output)
    ):
        parser.error("invalid counts, options, binaries, fixture directory or output path")
    allowed = sorted(os.sched_getaffinity(0))
    if len(allowed) < 2:
        parser.error("at least two allowed CPUs are required")
    benchmark_cpu, driver_cpu = allowed[0], allowed[-1]
    os.sched_setaffinity(0, {driver_cpu})
    sources = sorted([*Path("src").rglob("*.rs"), Path("Cargo.toml"), Path("Cargo.lock"),
                      Path("examples/index_memory_bench.rs"), Path("examples/checkpoint_rollover_bench.rs"),
                      Path("scripts/benchmarks/telemetry.py"), Path(__file__)])
    baseline = subprocess.check_output(["git", "rev-parse", "--verify", args.baseline_revision + "^{commit}"], text=True).strip()
    report = {
        "revision": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "baseline_revision": baseline,
        "git_status": subprocess.check_output(["git", "status", "--short"], text=True),
        "source_sha256": {str(path): sha256(path) for path in sources},
        "binary_sha256": sha256(args.binary), "baseline_binary_sha256": sha256(args.baseline_binary),
        "prepare_binary_sha256": sha256(args.prepare_binary),
        "binary_provenance": "Baseline revision is caller-supplied; build both revisions separately with identical benchmark examples.",
        "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "cpu_model": next((line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines() if line.startswith("model name")), "unknown"),
        "cpu_quota": read("/sys/fs/cgroup/cpu.max"), "allowed_cpus": allowed,
        "benchmark_cpu": benchmark_cpu, "driver_cpu": driver_cpu,
        "filesystem": json.loads(subprocess.check_output(["findmnt", "-J", "-T", args.directory, "-o", "TARGET,FSTYPE"], text=True)),
        "limitations": "Single Linux VM and filesystem, warm cache, one client, fresh processes, and same verified fixture. The timer includes managed checkpoint validation, synchronization and publication; reopening and sampled checks are outside it. RSS excludes page cache. VM/cgroup telemetry includes setup and verification. Not a competitor ranking or power-loss test.",
        "cases": [], "failure": None,
    }
    try:
        with tempfile.TemporaryDirectory(prefix=".managed-rollover-pair-", dir=args.directory) as root:
            for count in counts:
                parent = Path(root) / str(count)
                parent.mkdir()
                prefix = ["taskset", "-c", str(benchmark_cpu)]
                prepare = subprocess.run(prefix + [args.prepare_binary, "prepare", str(parent), str(count),
                                         str(args.key_bytes), str(args.tail_keys)], check=True, capture_output=True, text=True, timeout=300)
                info = fields(prepare.stdout)
                if int(info["keys"]) != count or int(info["tail_keys"]) != args.tail_keys:
                    raise RuntimeError("unexpected fixture")
                case = {"keys": count, "key_bytes": args.key_bytes, "tail_keys": args.tail_keys,
                        "log_bytes": int(info["log_bytes"]), "tail_bytes": int(info["tail_bytes"]),
                        "quiet_check": quiet_window(), "runs": []}
                report["cases"].append(case)
                reference_digest = None
                for round_index in range(args.rounds):
                    order = [("baseline", args.baseline_binary), ("candidate", args.binary)]
                    if round_index % 2:
                        order.reverse()
                    for label, binary in order:
                        before = snapshot()
                        command = prefix + [binary, str(parent), str(count), str(args.key_bytes),
                                            str(args.tail_keys), "managed", info["token"]]
                        process = subprocess.run(command, check=True, capture_output=True, text=True, timeout=300)
                        noise = difference(before, snapshot())
                        sample = fields(process.stdout)
                        if sample.get("mode") != "managed" or len(sample.get("digest", "")) != 64:
                            raise RuntimeError("unexpected benchmark output")
                        if reference_digest is None:
                            reference_digest = sample["digest"]
                        if sample["digest"] != reference_digest:
                            raise RuntimeError("checkpoint digests differ")
                        values = {key: int(sample[key]) for key in ("elapsed_us", "before_rss_kib", "after_rss_kib", "peak_rss_kib", "checkpoint_bytes")}
                        if values["peak_rss_kib"] < max(values["before_rss_kib"], values["after_rss_kib"]):
                            raise RuntimeError("invalid process RSS sample")
                        case["runs"].append({"round": round_index + 1, "variant": label, **values, "noise": noise})
                case["checkpoint_digest"] = reference_digest
                case["summary"] = {}
                for label in ("baseline", "candidate"):
                    samples = [run["elapsed_us"] for run in case["runs"] if run["variant"] == label]
                    case["summary"][label] = {"median_elapsed_us": statistics.median(samples), "min_elapsed_us": min(samples), "max_elapsed_us": max(samples)}
    except Exception as error:
        report["failure"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        report["finished_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        with open(args.output, "x", encoding="utf-8") as output:
            json.dump(report, output, indent=2)
            output.write("\n")
    for case in report["cases"]:
        print(f"keys={case['keys']} summary={case['summary']}")


if __name__ == "__main__":
    main()
