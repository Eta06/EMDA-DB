#!/usr/bin/env python3
"""Run the paired benchmark and retain subprocess logs and sampled disk peaks."""
import argparse
import json
import os
from pathlib import Path
import re
import signal
import stat
import subprocess
import sys
import time


def allocated_bytes(paths):
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
                pending.extend(path.iterdir())
        except FileNotFoundError:
            # Publication may move a private stage between two samples.
            continue
    return total


def command(binary, phase, fixture, backend, keys, epoch):
    if sys.platform == "darwin":
        timing = ["/usr/bin/time", "-l"]
    else:
        timing = ["/usr/bin/time", "-f", "peak_rss_kib=%M"]
    return timing + [str(binary), phase, str(fixture), backend, str(keys), str(epoch)]


def run(binary, directory, phase, backend, keys, epoch):
    fixture = directory / "fixture"
    paths = [fixture / backend, fixture / "authority"] if backend == "retained" else [
        fixture / "ordinary" if backend == "ordinary" else fixture
    ]
    logfile = directory / f"{epoch:02}-{backend}-{phase}.log"
    start = time.monotonic()
    initial = allocated_bytes(paths)
    peak = initial
    samples = 1
    with logfile.open("w") as out:
        child = subprocess.Popen(command(binary, phase, fixture, backend, keys, epoch),
                                 stdout=out, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            while child.poll() is None:
                peak = max(peak, allocated_bytes(paths))
                samples += 1
                time.sleep(0.025)
        finally:
            if child.poll() is None:
                os.killpg(child.pid, signal.SIGKILL)
                child.wait()
    duration = time.monotonic() - start
    final = allocated_bytes(paths)
    combined = logfile.read_text()
    metrics = {"phase": phase, "backend": backend, "keys": keys, "epoch": epoch,
               "exit": child.returncode, "observed_seconds": duration,
               "allocated_initial_bytes": initial, "allocated_final_bytes": final,
               "allocated_peak_sample_bytes": max(peak, final), "disk_samples": samples + 1}
    for name, value in re.findall(r"(\w+)=(\d+)", combined):
        metrics[name] = int(value)
    if sys.platform == "darwin":
        match = re.search(r"(\d+)\s+maximum resident set size", combined)
        if match:
            metrics["peak_rss_bytes"] = int(match[1])
    elif "peak_rss_kib" in metrics:
        metrics["peak_rss_bytes"] = metrics.pop("peak_rss_kib") * 1024
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
    args = parser.parse_args()
    if not 4096 <= args.keys <= 262144 or not 1 <= args.epochs <= 1000:
        parser.error("keys must be 4096–262144 and epochs must be 1–1000")
    binary, directory = args.binary.resolve(strict=True), args.new_directory.resolve()
    directory.mkdir()
    run(binary, directory, "setup", "both", args.keys, 0)
    run(binary, directory, "cutover", "retained", args.keys, 0)
    rotations = 0
    for epoch in range(1, args.epochs + 1):
        backends = ["ordinary", "retained"] if epoch % 2 else ["retained", "ordinary"]
        for backend in backends:
            metrics = run(binary, directory, "mutate", backend, args.keys, epoch)
            rotations += metrics.get("rotations", 0)
            run(binary, directory, "reopen", backend, args.keys, epoch)
            run(binary, directory, "verify", backend, args.keys, epoch)
        print(f"{directory.name}: verified both backends at epoch {epoch}", flush=True)
    print(f"rotations={rotations}", flush=True)


if __name__ == "__main__":
    main()
