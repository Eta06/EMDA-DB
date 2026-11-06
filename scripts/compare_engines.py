#!/usr/bin/env python3
"""Serial, affinity-controlled local database comparison with noise telemetry.

Prepare optional dependencies and Docker images first; this command does not
pull images or build code during measurement. All managed containers and data
are disposable and are removed in finally blocks. Results retain failed/noisy
runs. See docs/BENCHMARKS.md for scope and durability configuration.
"""
import argparse
import hashlib
import importlib.metadata
import json
import math
import os
from pathlib import Path
import platform
import subprocess
import sys
import time

from benchmarks.services import IMAGES, LocalService, docker
from benchmarks.telemetry import parse_cpus, quiet_window, read, snapshot

BACKENDS = ["prototype", "sqlite", "sqlite_python", "postgres", "redis", "mongo",
            "turso", "rocksdb", "lmdb", "duckdb", "foundationdb"]
# operations denotes cycles for CRUD and single operations otherwise.
CASES = [
    dict(name="single_mixed", clients=1, operations=1000, keys=250, write_percent=50, value_bytes=64, workload="mixed"),
    dict(name="concurrent_mixed", clients=4, operations=1000, keys=250, write_percent=50, value_bytes=64, workload="mixed"),
    dict(name="read_only", clients=4, operations=1500, keys=250, write_percent=0, value_bytes=64, workload="mixed"),
    dict(name="write_only", clients=4, operations=750, keys=250, write_percent=100, value_bytes=64, workload="mixed"),
    dict(name="mixed_4k", clients=4, operations=750, keys=250, write_percent=50, value_bytes=4096, workload="mixed"),
    dict(name="crud", clients=4, operations=200, keys=16, write_percent=0, value_bytes=64, workload="crud"),
]


def configure_cases(names=None, key_count=None, noise_calibration=False):
    available = {case["name"] for case in CASES}
    if noise_calibration and names:
        raise ValueError("noise calibration already selects its workload")
    if names and (len(names) != len(set(names)) or not set(names) <= available):
        raise ValueError("unknown or duplicate workload")
    if key_count is not None and not 1 <= key_count <= 100000:
        raise ValueError("key count must be between 1 and 100000")
    selected_names = {"concurrent_mixed"} if noise_calibration else set(names) if names else available
    selected = [case.copy() for case in CASES
                if case["name"] in selected_names]
    if key_count is not None:
        for case in selected:
            case["keys"] = key_count
    return selected


def validate_result(result, backend, case):
    multiplier = 4 if case["workload"] == "crud" else 1
    expected = {"backend": backend, "workload": case["workload"], "clients": case["clients"],
                "operations_per_client": case["operations"] * multiplier, "keys": case["keys"],
                "write_percent": case["write_percent"], "value_bytes": case["value_bytes"],
                "completed": case["clients"] * case["operations"] * multiplier}
    if any(result.get(key) != value for key, value in expected.items()):
        raise ValueError("benchmark result does not match the requested workload")
    counts = []
    for operation in ("read", "write", "create", "update", "delete"):
        values = result.get(operation)
        if not isinstance(values, dict) or type(values.get("count")) is not int or values["count"] < 0:
            raise ValueError("benchmark result has invalid operation counts")
        counts.append(values["count"])
        if values["count"]:
            percentiles = [values.get(name) for name in ("p50_us", "p95_us", "p99_us", "p99_9_us", "max_us")]
            if (any(type(value) not in (int, float) or not math.isfinite(value) or value < 0 for value in percentiles)
                    or percentiles != sorted(percentiles)):
                raise ValueError("benchmark result has invalid latency quantiles")
    throughput = result.get("ops_per_sec")
    if (sum(counts) != expected["completed"] or type(throughput) not in (int, float)
            or not math.isfinite(throughput) or throughput <= 0):
        raise ValueError("benchmark result has incomplete operations or throughput")
    return result


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--server", default="target/release/examples/network_bench_server")
    parser.add_argument("--rounds", type=int, default=4)
    parser.add_argument("--backends", nargs="+", choices=BACKENDS, default=BACKENDS)
    parser.add_argument("--client-cpus", default="0")
    parser.add_argument("--server-cpus", default="1-3")
    parser.add_argument("--housekeeping-cpus", default="4")
    parser.add_argument("--noise-calibration", action="store_true", help="paired unrestricted/pinned runs of concurrent_mixed only")
    parser.add_argument("--cases", nargs="+", choices=[case['name'] for case in CASES],
                        help="optional workload subset")
    parser.add_argument("--key-count", type=int, help="override the initialized key count for selected workloads")
    parser.add_argument("--case-timeout", type=int, default=180,
                        help="maximum seconds per workload including setup and verification")
    args = parser.parse_args()
    try:
        configured_cases = configure_cases(args.cases, args.key_count, args.noise_calibration)
    except ValueError as error:
        parser.error(str(error))
    allowed = os.sched_getaffinity(0)
    requested = parse_cpus(args.client_cpus) | parse_cpus(args.server_cpus) | parse_cpus(args.housekeeping_cpus)
    if not requested <= allowed or args.rounds < 2 or not Path(args.directory).is_dir() or not 1 <= args.case_timeout <= 1800:
        parser.error("invalid CPU affinity, rounds, or data directory")
    if parse_cpus(args.client_cpus) & parse_cpus(args.server_cpus):
        parser.error("client and server CPU sets must be disjoint")
    os.sched_setaffinity(0, parse_cpus(args.housekeeping_cpus))
    sources = sorted([*Path("scripts/benchmarks").glob("*.py"), Path(__file__), Path("scripts/network_compare.py"),
                      Path("scripts/embedded_bench_server.py"), *Path("examples").rglob("*.rs"),
                      *Path("src").rglob("*.rs"), Path("Cargo.toml"), Path("Cargo.lock"), Path("scripts/benchmark-requirements.txt")])
    result = {
        "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "revision": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "git_status": subprocess.check_output(["git", "status", "--short"], text=True),
        "source_sha256": {str(p): digest(p) for p in sources}, "server_sha256": digest(args.server),
        "platform": platform.platform(), "python": sys.version,
        "cpu_model": next((line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines() if line.startswith("model name")), "unknown"),
        "dependencies": {p: importlib.metadata.version(p) for p in ("psycopg", "redis", "pymongo", "pyturso", "rocksdict", "lmdb", "duckdb", "foundationdb")},
        "controls": {"client_cpus": args.client_cpus, "server_cpus": args.server_cpus, "housekeeping_cpus": args.housekeeping_cpus,
                     "allowed_cpus": sorted(allowed), "cpu_quota": read("/sys/fs/cgroup/cpu.max"),
                     "governor_visible": Path("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor").exists(),
                     "swap": read("/proc/swaps"), "one_backend_at_a_time": True, "quiet_gate_max_seconds": 5,
                     "noise_calibration": args.noise_calibration, "unrelated_host_services_modified": False,
                     "selected_cases": configured_cases, "case_timeout_seconds": args.case_timeout,
                     "notes": "No compilation, image pulls, cache drops, real-time priority, or disabled durability during measurement."},
        "filesystem": json.loads(subprocess.check_output(["findmnt", "-J", "-T", args.directory, "-o", "TARGET,FSTYPE"], text=True)),
        "initial_snapshot": snapshot(), "services": {}, "runs": [], "failures": [],
    }
    services = {}
    def save():
        Path(args.output).write_text(json.dumps(result, indent=2) + "\n")
    try:
        # Prepare stopped containers before the timed matrix; image downloads
        # are explicitly the caller's preparation step.
        for backend in args.backends:
            if backend in IMAGES:
                services[backend] = LocalService(backend, args.directory, args.server_cpus)
        for round_index in range(args.rounds):
            rotation = round_index % len(args.backends)
            order = args.backends[rotation:] + args.backends[:rotation]
            for backend in order:
                service = services.get(backend)
                if service:
                    service.start()
                    result["services"][backend] = service.metadata()
                env = os.environ.copy()
                if service:
                    env.update(service.env)
                try:
                    rotation = round_index % len(configured_cases)
                    cases = configured_cases[rotation:] + configured_cases[:rotation]
                    for case in cases:
                        modes = (["unrestricted", "pinned"] if round_index % 2 == 0 else ["pinned", "unrestricted"]) if args.noise_calibration else ["pinned"]
                        for mode in modes:
                            all_cpus = ",".join(map(str, sorted(allowed)))
                            client_cpus = all_cpus if mode == "unrestricted" else args.client_cpus
                            server_cpus = all_cpus if mode == "unrestricted" else args.server_cpus
                            if service:
                                docker("update", "--cpuset-cpus", server_cpus, service.name)
                            quiet = quiet_window()
                            command = [sys.executable, "scripts/network_compare.py", backend, "--server", args.server,
                                       "--directory", args.directory, "--client-cpus", client_cpus, "--server-cpus", server_cpus]
                            for key, value in case.items():
                                if key != "name":
                                    command += ["--" + key.replace("_", "-"), str(value)]
                            entry = {"backend": backend, "round": round_index, "mode": mode, "case": case,
                                     "command": command, "pre_run_quiet": quiet}
                            try:
                                run = subprocess.run(command, env=env, capture_output=True, text=True, timeout=args.case_timeout)
                                if run.returncode:
                                    raise RuntimeError(run.stderr[-4000:] or f"exit {run.returncode}")
                                entry["result"] = validate_result(json.loads(run.stdout), backend, case)
                            except Exception as error:
                                entry["error"] = str(error)
                                result["failures"].append({"backend": backend, "round": round_index, "case": case["name"], "error": str(error)})
                            result["runs"].append(entry)
                            save()
                    print(f"round={round_index + 1} backend={backend} runs={len(result['runs'])} failures={len(result['failures'])}", flush=True)
                finally:
                    if service:
                        service.stop()
        result["finished_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        result["final_snapshot"] = snapshot()
        save()
    finally:
        for service in services.values():
            service.close()
    if result["failures"]:
        raise SystemExit("Some measurements failed; see the retained failures in the result file")


if __name__ == "__main__":
    main()
