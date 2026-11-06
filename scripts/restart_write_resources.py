#!/usr/bin/env python3
"""Measure concurrent durable writes and verified fresh-process recovery on Linux."""
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
from index_memory_compare import fields

MODES = ("full", "checkpoint")


def digest(path):
    with Path(path).open("rb") as file:
        value = hashlib.sha256()
        for part in iter(lambda: file.read(1024 * 1024), b""):
            value.update(part)
    return value.hexdigest()


def validate_sample(sample, mode, phase, epoch, keys, tail, key_bytes, workers, writes):
    expected = {"mode": mode, "phase": phase, "epoch": epoch, "base_keys": keys,
                "tail_keys": tail, "key_bytes": key_bytes, "workers": workers,
                "writes_per_worker": writes, "prior_verified": epoch * workers * writes,
                "current_verified": workers * writes if phase == "write" else 0}
    if any(sample.get(key) != value for key, value in expected.items()):
        raise ValueError("benchmark returned unexpected configuration or verification counts")
    latencies = sample.get("write_latencies_ns")
    if (not isinstance(latencies, list) or len(latencies) != expected["current_verified"]
            or any(not isinstance(value, int) or value <= 0 for value in latencies)):
        raise ValueError("benchmark returned missing or invalid write latencies")
    for key in ("open_us", "rss_after_open_kib", "peak_rss_kib"):
        if not isinstance(sample.get(key), int) or sample[key] <= 0:
            raise ValueError("benchmark returned invalid resource metrics")
    elapsed = sample.get("write_elapsed_ns")
    if not isinstance(elapsed, int) or (elapsed <= 0 if phase == "write" else elapsed != 0):
        raise ValueError("benchmark returned invalid write elapsed time")
    counters = sample.get("process_io")
    if not isinstance(counters, dict) or any(
        not isinstance(counters.get(key), int) or counters[key] < 0
        for key in ("rchar", "wchar", "read_bytes", "write_bytes", "read_calls", "write_calls")
    ):
        raise ValueError("benchmark returned invalid process I/O counters")
    return sample


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", required=True, help="existing directory for disposable fixtures")
    parser.add_argument("--output", required=True, help="new JSON result file")
    parser.add_argument("--binary", default="target/release/examples/restart_write_bench")
    parser.add_argument("--prepare-binary", default="target/release/examples/index_memory_bench")
    parser.add_argument("--keys", default="10000,200000")
    parser.add_argument("--key-bytes", type=int, default=128)
    parser.add_argument("--tail-keys", type=int, default=1024)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--writes", type=int, default=128, help="durable writes per worker per epoch")
    parser.add_argument("--epochs", type=int, default=4, help="write processes per fixture, followed by a verification process")
    args = parser.parse_args()
    try:
        counts = [int(value) for value in args.keys.split(",")]
    except ValueError:
        parser.error("keys must be comma-separated integers")
    if not counts or len(set(counts)) != len(counts) or any(not 1 <= count <= 1_000_000 for count in counts):
        parser.error("key counts must be distinct and between 1 and 1000000")
    if (not 8 <= args.key_bytes <= 1024 or not 0 <= args.tail_keys <= 4096
            or not 1 <= args.workers <= 32 or not 1 <= args.writes <= 1024
            or not 1 <= args.epochs <= 32
            or args.tail_keys + args.workers * args.writes * args.epochs > 16_384
            or (args.tail_keys + args.workers * args.writes * args.epochs) * args.key_bytes > 4 * 1024 * 1024):
        parser.error("invalid size, worker, epoch or checkpoint overlay limit")
    if not Path(args.directory).is_dir() or not Path(args.binary).is_file() or not Path(args.prepare_binary).is_file():
        parser.error("an existing fixture directory and prebuilt binaries are required")
    if os.path.lexists(args.output):
        parser.error("output must not already exist")
    allowed = sorted(os.sched_getaffinity(0))
    if len(allowed) < 2:
        parser.error("at least two CPUs in the allowed affinity mask are required")
    bench_cpus, driver_cpu = allowed[:-1], allowed[-1]
    os.sched_setaffinity(0, {driver_cpu})
    sources = sorted([*Path("src").rglob("*.rs"), Path("Cargo.toml"), Path("Cargo.lock"),
                      Path("examples/restart_write_bench.rs"), Path("examples/index_memory_bench.rs"),
                      Path("scripts/index_memory_compare.py"), Path("scripts/benchmarks/telemetry.py"), Path(__file__)])
    result = {
        "revision": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "git_status": subprocess.check_output(["git", "status", "--short"], text=True),
        "source_sha256": {str(path): digest(path) for path in sources},
        "binary_sha256": digest(args.binary), "prepare_binary_sha256": digest(args.prepare_binary),
        "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "cpu_model": next((line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines() if line.startswith("model name")), "unknown"),
        "cpu_quota": read("/sys/fs/cgroup/cpu.max"), "allowed_cpus": allowed,
        "benchmark_cpus": bench_cpus, "driver_cpu": driver_cpu,
        "filesystem": json.loads(subprocess.check_output(["findmnt", "-J", "-T", args.directory, "-o", "TARGET,FSTYPE"], text=True)),
        "limitations": "One Linux VM/filesystem and warm caches; processes open and close cleanly, with no power-loss simulation. Different engine paths use separate equivalently populated fixtures. Workers perform independent, unique-key durable writes; no transaction, failure or external competitor is measured. Every earlier workload value is checked after each fresh-process open, and all final values are checked by an additional clean open; only sampled base/tail values are checked. RSS excludes page cache; process I/O includes open, verification and measurement overhead and is not physical I/O. Telemetry includes the VM/cgroup workload. Results do not establish resource, recovery or latency bounds.",
        "completed": False, "failure": None, "cases": [],
    }
    output = Path(args.output)
    with output.open("x", encoding="utf-8") as file:
        json.dump(result, file, indent=2)
        file.write("\n")

    def save():
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=output.parent,
                                         prefix=".restart-resource-report-", delete=False) as file:
            temporary = Path(file.name)
            try:
                json.dump(result, file, indent=2)
                file.write("\n")
                file.flush()
                os.replace(temporary, output)
            finally:
                temporary.unlink(missing_ok=True)

    affinity = ["taskset", "-c", ",".join(str(cpu) for cpu in bench_cpus)]
    try:
        with tempfile.TemporaryDirectory(prefix=".restart-resource-", dir=args.directory) as root:
            for count in counts:
                case = {"keys": count, "key_bytes": args.key_bytes, "tail_keys": args.tail_keys,
                        "workers": args.workers, "writes_per_worker": args.writes,
                        "epochs": args.epochs, "fixtures": {}, "runs": []}
                result["cases"].append(case)
                for mode in MODES:
                    parent = Path(root) / f"{count}-{mode}"
                    parent.mkdir()
                    prepared = subprocess.run(affinity + [args.prepare_binary, "prepare", str(parent), str(count), str(args.key_bytes), str(args.tail_keys)],
                                              check=True, text=True, capture_output=True, timeout=600)
                    info = fields(prepared.stdout)
                    if int(info["keys"]) != count or int(info["key_bytes"]) != args.key_bytes or int(info["tail_keys"]) != args.tail_keys:
                        raise RuntimeError("unexpected fixture configuration")
                    case["fixtures"][mode] = {"token": info["token"], "log_bytes": int(info["log_bytes"]),
                                              "checkpoint_bytes": int(info["checkpoint_bytes"]), "tail_bytes": int(info["tail_bytes"]),
                                              "log_sha256": digest(parent / "store" / "records.log"),
                                              "checkpoint_sha256": digest(parent / "index")}
                case["quiet_check"] = quiet_window()
                save()
                for epoch in range(args.epochs + 1):
                    phase = "write" if epoch < args.epochs else "verify"
                    modes = MODES if epoch % 2 == 0 else tuple(reversed(MODES))
                    for mode in modes:
                        parent = Path(root) / f"{count}-{mode}"
                        command = affinity + [args.binary, str(parent), str(count), str(args.tail_keys), str(args.key_bytes),
                                              mode, case["fixtures"][mode]["token"], str(args.workers), str(args.writes), str(epoch), phase]
                        before = snapshot()
                        process = subprocess.run(command, check=True, text=True, capture_output=True, timeout=600)
                        noise = difference(before, snapshot())
                        sample = validate_sample(json.loads(process.stdout), mode, phase, epoch, count, args.tail_keys,
                                                 args.key_bytes, args.workers, args.writes)
                        case["runs"].append({**sample, "noise": noise})
                        save()
                case["summary"] = {}
                for mode in MODES:
                    runs = [run for run in case["runs"] if run["mode"] == mode]
                    writes = [run for run in runs if run["phase"] == "write"]
                    latencies = [ns for run in writes for ns in run["write_latencies_ns"]]
                    sorted_latencies = sorted(latencies)
                    case["summary"][mode] = {
                        "median_open_us": statistics.median(run["open_us"] for run in runs),
                        "max_peak_rss_kib": max(run["peak_rss_kib"] for run in runs),
                        "median_write_latency_ns": statistics.median(latencies),
                        "p99_write_latency_ns_nearest_rank": sorted_latencies[(99 * len(sorted_latencies) + 99) // 100 - 1],
                        "median_write_ops_per_second": statistics.median(len(run["write_latencies_ns"]) * 1e9 / run["write_elapsed_ns"] for run in writes),
                        "final_verified_workload_keys": runs[-1]["prior_verified"],
                    }
                save()
        result["completed"] = True
    except Exception as error:
        result["failure"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        result["finished_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        save()
    for case in result["cases"]:
        print(f"keys={case['keys']} summary={case['summary']}")


if __name__ == "__main__":
    main()
