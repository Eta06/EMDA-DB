#!/usr/bin/env python3
"""Paired, affinity-controlled warm-cache checkpoint versus full replay benchmark."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import selectors
import statistics
import subprocess
import sys
import time

from benchmarks.telemetry import difference, parse_cpus, quiet_window, read, snapshot

CASES = [("churn", 100_000, 1_000), ("higher_key_count", 25_000, 10_000)]


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--binary", default="target/release/examples/checkpoint_bench")
    parser.add_argument("--rounds", type=int, default=6)
    parser.add_argument("--bench-cpus", default="1-3")
    parser.add_argument("--housekeeping-cpus", default="4")
    args = parser.parse_args()
    allowed = os.sched_getaffinity(0)
    benchmark_cpus = parse_cpus(args.bench_cpus)
    housekeeping_cpus = parse_cpus(args.housekeeping_cpus)
    if not (benchmark_cpus | housekeeping_cpus) <= allowed or benchmark_cpus & housekeeping_cpus or not 2 <= args.rounds <= 100:
        parser.error("invalid CPU sets or round count")
    if not Path(args.directory).is_dir() or not Path(args.binary).is_file():
        parser.error("existing directory and prebuilt example required")
    os.sched_setaffinity(0, housekeeping_cpus)
    sources = sorted([*Path("src").rglob("*.rs"), Path("examples/checkpoint_bench.rs"), Path("scripts/benchmarks/telemetry.py"), Path(__file__), Path("Cargo.toml"), Path("Cargo.lock")])
    result = {
        "revision": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "git_status": subprocess.check_output(["git", "status", "--short"], text=True),
        "source_sha256": {str(path): sha256(path) for path in sources},
        "binary_sha256": sha256(args.binary),
        "cpu_model": next((line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines() if line.startswith("model name")), "unknown"),
        "cpu_quota": read("/sys/fs/cgroup/cpu.max"), "benchmark_cpus": sorted(benchmark_cpus),
        "housekeeping_cpus": sorted(housekeeping_cpus), "allowed_cpus": sorted(allowed),
        "filesystem": json.loads(subprocess.check_output(["findmnt", "-J", "-T", args.directory, "-o", "TARGET,FSTYPE"], text=True)),
        "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "cases": [],
        "limitations": "One VM, warm cache, closed-loop direct opens; process/VM noise counters include the workload. The checkpoint is explicitly trusted and the full prefix is hashed. Not a power-loss or memory-efficiency test.",
    }
    env = os.environ.copy()
    env["EMDA_CHECKPOINT_BENCH_GATE"] = "1"
    for name, writes, keys in CASES:
        command = ["taskset", "-c", args.bench_cpus, args.binary, args.directory, str(writes), str(keys), str(args.rounds)]
        process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1, env=env)
        try:
            # Fixture preparation is outside the quiet window and measurements.
            with selectors.DefaultSelector() as selector:
                selector.register(process.stdout, selectors.EVENT_READ)
                prelude = bytearray()
                while True:
                    if not selector.select(60):
                        raise RuntimeError("checkpoint benchmark preparation timed out")
                    chunk = os.read(process.stdout.fileno(), 4096)
                    if not chunk:
                        raise RuntimeError("benchmark exited before measurement gate")
                    prelude.extend(chunk)
                    if b"READY_FOR_MEASUREMENT\n" in prelude:
                        break
                before_gate, after_gate = bytes(prelude).split(b"READY_FOR_MEASUREMENT\n", 1)
                if after_gate:
                    raise RuntimeError("unexpected output after measurement gate")
                lines = before_gate.decode("utf-8").splitlines()
            quiet = quiet_window()
            before = snapshot()
            process.stdin.write("\n")
            process.stdin.flush()
            output, error = process.communicate(timeout=120)
            after = snapshot()
            if process.returncode:
                raise RuntimeError(error[-4000:] or f"exit {process.returncode}")
            samples = []
            for line in output.splitlines():
                match = re.fullmatch(r"round=(\d+) mode=(full|checkpoint) open_us=(\d+)", line)
                if not match:
                    raise RuntimeError(f"unexpected benchmark output: {line!r}")
                samples.append({"round": int(match.group(1)), "mode": match.group(2), "open_us": int(match.group(3))})
            if len(samples) != args.rounds * 2 or any(sum(sample["mode"] == mode for sample in samples) != args.rounds for mode in ("full", "checkpoint")):
                raise RuntimeError("incomplete paired measurements")
            summary = {}
            for mode in ("full", "checkpoint"):
                values = [item["open_us"] for item in samples if item["mode"] == mode]
                summary[mode] = {"median_us": statistics.median(values), "min_us": min(values), "max_us": max(values)}
            result["cases"].append({"name": name, "writes": writes, "keys": keys, "command": command, "setup_output": lines,
                                    "pre_run_quiet": quiet, "noise": difference(before, after), "samples": samples, "summary": summary})
            Path(args.output).write_text(json.dumps(result, indent=2) + "\n")
            print(name, summary, flush=True)
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()
    result["finished_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    Path(args.output).write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
