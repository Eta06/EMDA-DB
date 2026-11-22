#!/usr/bin/env python3
"""Run the paired benchmark and retain subprocess logs and sampled disk peaks."""
import argparse
import hashlib
from itertools import permutations
import json
import os
from pathlib import Path
import platform
import re
import signal
import stat
import subprocess
import sys
import time


def allocated_bytes(paths, excluded=()):
    """Count allocated blocks once per inode, including any visible stages."""
    pending = list(paths)
    seen = set()
    total = 0
    while pending:
        path = pending.pop()
        try:
            metadata = path.lstat()
            identity = (metadata.st_dev, metadata.st_ino)
            if identity in seen:
                continue
            seen.add(identity)
            total += metadata.st_blocks * 512
            if stat.S_ISDIR(metadata.st_mode):
                pending.extend(child for child in path.iterdir() if child not in excluded)
        except FileNotFoundError:
            # Publication may move a private stage between two samples.
            continue
    return total


def command(binary, phase, fixture, backend, keys, epoch):
    if sys.platform == "darwin":
        timing = ["/usr/bin/time", "-lp"]
    else:
        timing = ["/usr/bin/time", "-f",
                  "peak_rss_kib=%M\ncpu_user_seconds=%U\ncpu_system_seconds=%S"]
    return timing + [str(binary), phase, str(fixture), backend, str(keys), str(epoch)]


def run(binary, directory, phase, backend, keys, epoch):
    fixture = directory / "fixture"
    excluded = ()
    if phase in {"bind-archive", "lifecycle"}:
        paths = [fixture]
        excluded = (fixture / "ordinary", fixture / "sqlite")
    elif backend == "retained":
        paths = [fixture / backend, fixture / "authority"]
    elif phase == "setup":
        paths = [fixture]
    else:
        paths = [fixture / backend]
    logfile = directory / f"{epoch:02}-{backend}-{phase}.log"
    start = time.monotonic()
    initial = allocated_bytes(paths, excluded)
    peak = initial
    samples = 1
    with logfile.open("w") as out:
        child = subprocess.Popen(command(binary, phase, fixture, backend, keys, epoch),
                                 stdout=out, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            while child.poll() is None:
                peak = max(peak, allocated_bytes(paths, excluded))
                samples += 1
                time.sleep(0.025)
        finally:
            if child.poll() is None:
                os.killpg(child.pid, signal.SIGKILL)
                child.wait()
    duration = time.monotonic() - start
    final = allocated_bytes(paths, excluded)
    combined = logfile.read_text()
    metrics = {"phase": phase, "backend": backend, "keys": keys, "epoch": epoch,
               "exit": child.returncode, "observed_seconds": duration,
               "allocated_initial_bytes": initial, "allocated_final_bytes": final,
               "allocated_peak_sample_bytes": max(peak, final), "disk_samples": samples + 1}
    for name, value in re.findall(r"(\w+)=(\d+)(?=\s|$)", combined):
        metrics[name] = int(value)
    version = re.search(r"sqlite_version=(\d+\.\d+\.\d+)", combined)
    if version:
        metrics["sqlite_version"] = version[1]
    if sys.platform == "darwin":
        match = re.search(r"(\d+)\s+maximum resident set size", combined)
        if match:
            metrics["peak_rss_bytes"] = int(match[1])
        for name, value in re.findall(r"^(user|sys) (\d+\.\d+)$", combined, re.MULTILINE):
            metrics["cpu_user_seconds" if name == "user" else "cpu_system_seconds"] = float(value)
    elif "peak_rss_kib" in metrics:
        metrics["peak_rss_bytes"] = metrics.pop("peak_rss_kib") * 1024
        for name, value in re.findall(r"(cpu_\w+_seconds)=(\d+\.\d+)", combined):
            metrics[name] = float(value)
    with (directory / "measurements.jsonl").open("a") as output:
        print(json.dumps(metrics, sort_keys=True), file=output)
    if child.returncode != 0 or "peak_rss_bytes" not in metrics:
        raise SystemExit(f"{phase} {backend} epoch {epoch} failed, see {logfile}")
    return metrics


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("new_directory", type=Path)
    parser.add_argument("keys", type=int)
    parser.add_argument("epochs", type=int)
    parser.add_argument("--sqlite", action="store_true", help="include a WAL/FULL SQLite control")
    parser.add_argument("--retire-and-restore", action="store_true", help="verify a local backup, retire the legacy archive, and restore the final state")
    args = parser.parse_args()
    if not 4096 <= args.keys <= 1048576 or not 1 <= args.epochs <= 1000:
        parser.error("keys must be 4096–1048576 and epochs must be 1–1000")
    binary, directory = args.binary.resolve(strict=True), args.new_directory.resolve()
    directory.mkdir()
    backends = ["ordinary", "retained", "sqlite"] if args.sqlite else ["ordinary", "retained"]
    orders = tuple(permutations(backends))
    provenance = {
        "platform": platform.platform(), "binary": str(binary),
        "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
        "runner_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "keys": args.keys, "epochs": args.epochs, "backend_orders": orders,
        "suffix_work_override": os.environ.get("EMDA_BENCH_SUFFIX_WORK"),
        "retire_and_restore": args.retire_and_restore,
    }
    (directory / "environment.json").write_text(json.dumps(provenance, indent=2) + "\n")
    run(binary, directory, "setup", "all" if args.sqlite else "both", args.keys, 0)
    if args.retire_and_restore:
        run(binary, directory, "bind-archive", "retained", args.keys, 0)
    run(binary, directory, "cutover", "retained", args.keys, 0)
    rotations = 0
    for epoch in range(1, args.epochs + 1):
        for backend in orders[(epoch - 1) % len(orders)]:
            metrics = run(binary, directory, "mutate", backend, args.keys, epoch)
            rotations += metrics.get("rotations", 0)
            run(binary, directory, "reopen", backend, args.keys, epoch)
            run(binary, directory, "verify", backend, args.keys, epoch)
        print(f"{directory.name}: verified {len(backends)} backends at epoch {epoch}", flush=True)
    print(f"rotations={rotations}", flush=True)
    if args.retire_and_restore:
        run(binary, directory, "lifecycle", "retained", args.keys, args.epochs)
        print("verified restored and current retained state", flush=True)


if __name__ == "__main__":
    main()
