#!/usr/bin/env python3
"""Repeat serial SQLite / baseline / candidate latency comparisons.

Build release examples in both revisions before running. All stores are fresh,
all engines synchronize before acknowledgement, and engine order rotates between
rounds. No performance assertions: results depend on storage and scheduling.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import time


def command(args):
    return subprocess.check_output(args, text=True, timeout=180).strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-directory", required=True)
    parser.add_argument("--baseline-revision", required=True)
    parser.add_argument("--candidate-directory", default="target/release/examples")
    parser.add_argument("--directory", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--rounds", type=int, default=4)
    args = parser.parse_args()
    if args.rounds < 2 or not Path(args.directory).is_dir():
        parser.error("at least two rounds and an existing data directory are required")
    paths = {"baseline": Path(args.baseline_directory).resolve(),
             "candidate": Path(args.candidate_directory).resolve()}
    binaries = {}
    for label, directory in paths.items():
        for name in ("kv_bench", "network_bench_server"):
            binary = directory / name
            binaries[f"{label}/{name}"] = hashlib.sha256(binary.read_bytes()).hexdigest()
    sources = sorted([*Path("src").rglob("*.rs"), *Path("examples").rglob("*.rs"),
                      Path("Cargo.toml"), Path("Cargo.lock"), Path(__file__),
                      Path("scripts/network_compare.py")])
    result = {
        "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "baseline_revision": args.baseline_revision,
        "candidate_head": command(["git", "rev-parse", "HEAD"]),
        "candidate_status": command(["git", "status", "--short"]),
        "source_sha256": {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in sources},
        "binary_sha256": binaries, "platform": platform.platform(),
        "cpu_count": os.cpu_count(), "rustc": command(["rustc", "-Vv"]),
        "cpu_model": next((line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines() if line.startswith("model name")), "unknown"),
        "cpu_max": Path("/sys/fs/cgroup/cpu.max").read_text().strip() if Path("/sys/fs/cgroup/cpu.max").exists() else "unknown",
        "directory": str(Path(args.directory).resolve()),
        "filesystem": json.loads(command(["findmnt", "-J", "-T", args.directory, "-o", "TARGET,FSTYPE"])),
        "limitations": ["Closed-loop, warm data, fresh stores, single host",
                        "Virtualized storage; underlying durability not independently verified",
                        "SQLite WAL/FULL; prototype sync-before-ack; feature sets differ",
                        "Clients are not latency SLO or coordinated-omission measurements"],
        "runs": [],
    }
    # clients, operations per client, keys, write percentage, value bytes
    cases = [(1, 2000, 250, 50, 64), (4, 1000, 250, 50, 64), (8, 1000, 250, 50, 64),
             (1, 1500, 250, 100, 64), (4, 1000, 250, 100, 64),
             (4, 2000, 250, 0, 64), (4, 1000, 250, 50, 4096),
             (4, 250, 100, 50, 131072)]
    for transport in ("direct", "loopback"):
        for case in cases:
            clients, operations, keys, writes, size = case
            for round_index in range(args.rounds):
                labels = ["baseline", "candidate", "sqlite"]
                rotation = round_index % len(labels)
                labels = labels[rotation:] + labels[:rotation]
                for label in labels:
                    backend = "sqlite" if label == "sqlite" else "prototype"
                    directory = paths["baseline" if label == "baseline" else "candidate"]
                    if transport == "direct":
                        argv = [str(directory / "kv_bench"), backend, str(clients), str(operations),
                                str(keys), str(writes), args.directory, str(size)]
                        measured = {"stdout": command(argv)}
                    else:
                        argv = ["python3", "scripts/network_compare.py", backend,
                                "--server", str(directory / "network_bench_server"),
                                "--directory", args.directory, "--clients", str(clients),
                                "--operations", str(operations), "--keys", str(keys),
                                "--write-percent", str(writes), "--value-bytes", str(size)]
                        measured = json.loads(command(argv))
                    result["runs"].append({"label": label, "transport": transport,
                                           "round": round_index, "case": case,
                                           "command": argv, "result": measured})
                    # Preserve completed runs even if a later invocation fails.
                    Path(args.output).write_text(json.dumps(result, indent=2) + "\n")
            print(f"completed {transport} {case}: {len(result['runs'])} runs", flush=True)
    result["finished_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    Path(args.output).write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
