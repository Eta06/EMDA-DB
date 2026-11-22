#!/usr/bin/env python3
"""Repeat the in-process concurrent document benchmark on a Linux fixture filesystem."""
import argparse
import gzip
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import subprocess
import time

from benchmarks.telemetry import difference, snapshot

ENGINES = ("store", "checkpoint", "sqlite")


def validate(result, engine, clients, cycles, documents):
    if (result.get("engine") != engine or result.get("clients") != clients
            or result.get("cycles_per_client") != cycles or result.get("documents") != documents
            or result.get("reopen_verified") is not True):
        raise ValueError("benchmark identity or reopen verification mismatch")
    throughput = result.get("ops_per_sec")
    if type(throughput) not in (int, float) or not math.isfinite(throughput) or throughput <= 0:
        raise ValueError("invalid benchmark throughput")
    samples = result.get("clients_samples", [])
    if len(samples) != clients:
        raise ValueError("incorrect client sample count")
    for client in samples:
        for name in ("update_ns", "query_ns"):
            values = client.get(name, [])
            if len(values) != cycles or any(type(value) is not int or value < 0 for value in values):
                raise ValueError(f"invalid samples for {name}")
        for name in ("update_retries", "query_retries"):
            if type(client.get(name)) is not int or client[name] < 0:
                raise ValueError(f"invalid count for {name}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--directory", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--source-revision", required=True)
    parser.add_argument("--baseline-binary", type=Path)
    parser.add_argument("--baseline-revision")
    parser.add_argument("--clients", default="1,2,4")
    parser.add_argument("--rounds", type=int, default=4)
    parser.add_argument("--cycles", type=int, default=100)
    parser.add_argument("--documents", type=int, default=500)
    args = parser.parse_args()
    try:
        clients = [int(value) for value in args.clients.split(",")]
    except ValueError:
        parser.error("clients must be comma-separated integers")
    if (not args.binary.is_file() or not args.directory.is_dir() or len(set(clients)) != len(clients)
            or not all(1 <= count <= 8 for count in clients)
            or not 2 <= args.rounds <= 20 or not 1 <= args.cycles <= 1000
            or not max(clients) <= args.documents <= 1000
            or bool(args.baseline_binary) != bool(args.baseline_revision)
            or (args.baseline_binary is not None and not args.baseline_binary.is_file())):
        parser.error("invalid benchmark path or workload dimensions")
    root = Path(__file__).resolve().parents[1]
    hashed = {
        "binary": args.binary,
        "driver": Path(__file__),
        "example": root / "examples/document_contention_bench.rs",
    }
    variants = [(engine, engine, args.binary) for engine in ENGINES]
    if args.baseline_binary is not None:
        hashed["baseline_binary"] = args.baseline_binary
        variants.extend(
            (f"{engine}-before", engine, args.baseline_binary)
            for engine in ENGINES if engine != "sqlite"
        )
    fingerprints = {name: hashlib.sha256(path.read_bytes()).hexdigest() for name, path in hashed.items()}
    if fingerprints.get("baseline_binary") == fingerprints["binary"]:
        parser.error("baseline and candidate binaries are identical")
    report = {
        "source_revision": args.source_revision,
        "baseline_revision": args.baseline_revision,
        "sha256": fingerprints,
        "platform": platform.platform(),
        "cpu_affinity": sorted(os.sched_getaffinity(0)),
        "filesystem": subprocess.check_output(
            ["findmnt", "-T", args.directory, "-n", "-o", "TARGET,FSTYPE"], text=True
        ).strip(),
        "clients": clients,
        "rounds": args.rounds,
        "cycles_per_client": args.cycles,
        "documents": args.documents,
        "runs": [],
    }
    with gzip.open(args.output, "xt", encoding="utf-8") as output:
        try:
            for dimension, count in enumerate(clients):
                for round_index in range(args.rounds):
                    offset = (dimension + round_index) % len(variants)
                    order = variants[offset:] + variants[:offset]
                    for label, engine, binary in order:
                        command = [str(binary.resolve()), engine, str(args.directory.resolve()),
                                   str(count), str(args.cycles), str(args.documents)]
                        before = snapshot()
                        completed = subprocess.run(command, text=True, capture_output=True, timeout=360, check=False)
                        run = {"variant": label, "round": round_index + 1, "command": command,
                               "telemetry": difference(before, snapshot()), "returncode": completed.returncode,
                               "stderr": completed.stderr}
                        report["runs"].append(run)
                        if completed.returncode != 0:
                            raise ValueError(f"{label} with {count} clients failed: {completed.stderr}")
                        result = json.loads(completed.stdout)
                        validate(result, engine, count, args.cycles, args.documents)
                        run["result"] = result
        except (ValueError, OSError, subprocess.TimeoutExpired) as error:
            report["failure"] = str(error)
            raise
        finally:
            json.dump(report, output)
            output.write("\n")


if __name__ == "__main__":
    main()
