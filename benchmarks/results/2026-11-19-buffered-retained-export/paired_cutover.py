"""Measure verified retained cutovers from copies of the same seeded log."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time


def digest(file):
    with file.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest() if hasattr(hashlib, "file_digest") else hashlib.sha256(stream.read()).hexdigest()


def invoke(binary, phase, fixture, keys, output, prefix=()):
    args = [*prefix, binary, phase, fixture, "retained", str(keys), "0"]
    start = time.perf_counter_ns()
    with output.open("w") as log:
        subprocess.run(args, stdout=log, stderr=subprocess.STDOUT, check=True)
    return time.perf_counter_ns() - start


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("baseline", type=Path)
    parser.add_argument("buffered", type=Path)
    parser.add_argument("--trace", action="store_true")
    args = parser.parse_args()
    root = args.directory.resolve()
    root.mkdir()
    candidates = {"baseline": args.baseline.resolve(strict=True), "buffered": args.buffered.resolve(strict=True)}
    keys = 65536
    seed = root / "seed"
    invoke(candidates["baseline"], "setup", seed, keys, root / "setup.log")
    ordinary = seed / "retained"
    source_hash = digest(ordinary / "records.log")
    witness_hash = digest(ordinary / "commit.state")
    magic = (ordinary / "records.log").open("rb").read(8)
    source_xattr = os.getxattr(ordinary / "records.log", "user.db.commit.v3") if magic == b"EMDALOG3" else None
    normalized = None

    def run(build, label, trace=False):
        nonlocal normalized
        run = Path(tempfile.mkdtemp(prefix=label + "-", dir=root))
        subprocess.run(["cp", "-a", ordinary, run / "retained"], check=True)
        timing = ["/usr/bin/time", "-l"] if sys.platform == "darwin" else ["/usr/bin/time", "-f", "peak_rss_kib=%M"]
        prefix = ["strace", "-qq", "-c", "-e", "trace=read,pread64,write,pwrite64,fsync,fdatasync", "-o", root / f"{label}.syscalls"] if trace else timing
        elapsed = invoke(candidates[build], "cutover", run, keys, root / f"{label}-cutover.log", prefix)
        catalog, = (run / "retained").glob("retained-cutover-*")
        assert digest(catalog / "legacy.records") == source_hash
        assert digest(catalog / "legacy.witness") == witness_hash
        if source_xattr is not None:
            assert os.getxattr(catalog / "legacy.records", "user.db.commit.v3") == source_xattr
        with (run / "retained" / "records.log").open("rb") as marker:
            assert marker.read(8) == b"EMDALOG4"
        base = (catalog / "generation-a/base").read_bytes()
        # Fresh random base UUID and the dependent trailer differ per cutover.
        stable_entries = hashlib.sha256(base[:32] + base[48:-32]).hexdigest()
        if normalized is None:
            normalized = stable_entries
        assert stable_entries == normalized
        invoke(candidates[build], "verify", run, keys, root / f"{label}-verify.log")
        assert f"verified_entries={keys}\n" in (root / f"{label}-verify.log").read_text()
        with (root / "measurements.jsonl").open("a") as results:
            print(json.dumps({"build": build, "case": label, "traced": trace, "ns": elapsed,
                              "keys": keys, "source_format": magic.decode(), "base_bytes": len(base),
                              "source_sha256": source_hash, "witness_sha256": witness_hash,
                              "normalized_base_sha256": stable_entries}), file=results)
        shutil.rmtree(run)

    for pair in range(4):
        for build in (("baseline", "buffered") if pair % 2 == 0 else ("buffered", "baseline")):
            run(build, f"{pair}-{build}")
    if args.trace:
        for build in candidates:
            run(build, f"traced-{build}", trace=True)


if __name__ == "__main__":
    main()
