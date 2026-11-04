#!/usr/bin/env python3
"""Compare verified warm-cache reopening of identical witnessed stores."""
import argparse
import errno
import hashlib
import json
import os
from pathlib import Path
import platform
import statistics
import subprocess
import tempfile
import time

from benchmarks.telemetry import difference, quiet_window, read, snapshot


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def revision(path):
    return subprocess.check_output(["git", "-C", str(path), "rev-parse", "HEAD"], text=True).strip()


def clean(path):
    return not subprocess.check_output(["git", "-C", str(path), "status", "--porcelain"], text=True).strip()


def identity(path):
    log = path / "records.log"
    try:
        metadata = hashlib.sha256(os.getxattr(log, "user.db.commit.v3")).hexdigest()
    except OSError as error:
        if error.errno not in {errno.ENODATA, errno.ENOTSUP, errno.EOPNOTSUPP}:
            raise
        metadata = None
    return {"log_sha256": sha(log), "witness_sha256": sha(path / "commit.state"),
            "metadata_sha256": metadata, "log_bytes": log.stat().st_size}


def parse(output, records, repetitions, log_bytes):
    lines = output.strip().splitlines()
    if len(lines) != 2 or not lines[1].startswith("open_samples_us="):
        raise ValueError("unexpected recovery benchmark output")
    fields = dict(item.split("=", 1) for item in lines[0].split())
    samples = json.loads(lines[1].split("=", 1)[1])
    if (int(fields["expected_records"]) != records or int(fields["repetitions"]) != repetitions
            or int(fields["value_bytes"]) != 64 or int(fields["log_bytes"]) != log_bytes
            or not isinstance(samples, list) or len(samples) != repetitions
            or any(type(value) is not int or value < 0 for value in samples)
            or samples != sorted(samples)):
        raise ValueError("invalid workload identity, samples, or median")
    for percentile in (50, 95, 99):
        index = (len(samples) * percentile + 99) // 100 - 1
        if int(fields[f"open_p{percentile}_us"]) != samples[index]:
            raise ValueError("reported percentile does not match samples")
    if int(fields["open_max_us"]) != samples[-1]:
        raise ValueError("reported maximum does not match samples")
    return {"samples_us": samples, "p50_us": int(fields["open_p50_us"]),
            "p95_us": int(fields["open_p95_us"]), "p99_us": int(fields["open_p99_us"]),
            "max_us": int(fields["open_max_us"])}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", required=True)
    parser.add_argument("--baseline-source", required=True)
    parser.add_argument("--baseline-binary", required=True)
    parser.add_argument("--candidate-binary", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--repetitions", type=int, default=50)
    args = parser.parse_args()
    baseline = Path(args.baseline_source).resolve()
    candidate = Path.cwd().resolve()
    cpus = sorted(os.sched_getaffinity(0))
    if (not Path(args.directory).is_dir() or not baseline.is_dir()
            or not Path(args.baseline_binary).is_file() or not Path(args.candidate_binary).is_file()
            or os.path.lexists(args.output) or not 2 <= args.rounds <= 50
            or not 1 <= args.repetitions <= 100 or len(cpus) < 2
            or not clean(baseline) or not clean(candidate)):
        parser.error("invalid paths, rounds, repetition count, CPU availability or dirty source")
    binaries = {"baseline": args.baseline_binary, "candidate": args.candidate_binary}
    binary_hashes = {name: sha(path) for name, path in binaries.items()}
    if binary_hashes["baseline"] == binary_hashes["candidate"]:
        parser.error("binaries are identical; build in separate target directories")
    for name in ("examples/recovery_bench.rs", "Cargo.lock"):
        if sha(baseline / name) != sha(candidate / name):
            parser.error(f"baseline and candidate differ in benchmark input {name}")
    benchmark_cpu, driver_cpu = cpus[0], cpus[-1]
    os.sched_setaffinity(0, {driver_cpu})
    output = Path(args.output)
    report = {"baseline_revision": revision(baseline), "candidate_revision": revision(candidate),
              "binary_sha256": binary_hashes, "script_sha256": sha(__file__),
              "benchmark_source_sha256": sha(candidate / "examples/recovery_bench.rs"),
              "cargo_lock_sha256": sha(candidate / "Cargo.lock"),
              "rustc_version": subprocess.check_output(["rustc", "+stable", "--version"], text=True).strip(),
              "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
              "platform": platform.platform(), "cpu_quota": read("/sys/fs/cgroup/cpu.max"),
              "cpu_model": next((line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines() if line.startswith("model name")), "unknown"),
              "benchmark_cpu": benchmark_cpu, "driver_cpu": driver_cpu,
              "filesystem": json.loads(subprocess.check_output(["findmnt", "-J", "-T", args.directory, "-o", "TARGET,FSTYPE"], text=True)),
              "scope": "Single-client warm-cache reopen of the same prebuilt, witnessed store; each process validates sampled values after each open. ABBA pass order. This does not measure new store creation, rejected corruption, physical power loss, off-host restore, competitor performance, or total memory.",
              "rounds": args.rounds, "repetitions": args.repetitions, "cases": [], "failures": []}
    with output.open("x", encoding="utf-8") as file:
        json.dump(report, file, indent=2)
        file.write("\n")

    def save():
        output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")

    with tempfile.TemporaryDirectory(prefix="emda-open-comparison-", dir=args.directory) as root:
        cases = {}
        for records in (1, 1000):
            path = Path(root) / f"store-{records}"
            subprocess.run(["taskset", "-c", str(benchmark_cpu), binaries["baseline"],
                            "prepare", str(path), str(records), "64"], check=True, capture_output=True, text=True, timeout=300)
            case = {"records": records, "fixture": identity(path), "runs": []}
            report["cases"].append(case)
            cases[records] = (path, case)
        save()
        for pass_index, variant in enumerate(("baseline", "candidate", "candidate", "baseline")):
            for round_index in range(args.rounds):
                records_order = (1, 1000) if (pass_index + round_index) % 2 == 0 else (1000, 1)
                for records in records_order:
                    path, case = cases[records]
                    run = {"pass": pass_index + 1, "round": round_index + 1, "variant": variant,
                           "pre_run_quiet": quiet_window(seconds=.25, attempts=3)}
                    try:
                        before = snapshot()
                        try:
                            command = ["taskset", "-c", str(benchmark_cpu), binaries[variant],
                                       "measure", str(path), str(records), str(args.repetitions), "64"]
                            process = subprocess.run(command, capture_output=True, text=True, timeout=300, check=True)
                            run["measurement"] = parse(process.stdout, records, args.repetitions, case["fixture"]["log_bytes"])
                            run["command"] = command
                        finally:
                            run["noise"] = difference(before, snapshot())
                        if identity(path) != case["fixture"]:
                            raise RuntimeError("recovery measurement changed the fixture")
                    except Exception as error:
                        run["error"] = f"{type(error).__name__}: {error}"
                        report["failures"].append({"records": records, "pass": pass_index + 1,
                                                   "round": round_index + 1, "variant": variant, "error": run["error"]})
                    finally:
                        case["runs"].append(run)
                        save()
        if report["failures"]:
            raise SystemExit("recovery comparison failed; partial report preserved")
        for case in report["cases"]:
            case["summary"] = {}
            for variant in binaries:
                runs = [run["measurement"]["p50_us"] for run in case["runs"] if run["variant"] == variant]
                case["summary"][variant] = {"median_of_p50_us": statistics.median(runs),
                                             "min_p50_us": min(runs), "max_p50_us": max(runs)}
        report["finished_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        save()
    print(f"Completed {sum(len(case['runs']) for case in report['cases'])} verified recovery runs")


if __name__ == "__main__":
    main()
