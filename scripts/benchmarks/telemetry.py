"""Read-only Linux noise telemetry. Unsupported counters remain explicit."""
import os
from pathlib import Path
import time


def parse_cpus(value):
    cpus = set()
    for part in value.split(","):
        limits = part.split("-")
        if len(limits) > 2:
            raise ValueError("invalid CPU range")
        first, last = int(limits[0]), int(limits[-1])
        if first < 0 or last < first or last > 65535:
            raise ValueError("invalid CPU range")
        cpus.update(range(first, last + 1))
    return cpus


def read(path):
    try:
        return Path(path).read_text().strip()
    except OSError:
        return None


def snapshot():
    cpu_stat = read("/sys/fs/cgroup/cpu.stat")
    proc_stat = read("/proc/stat")
    io = read("/sys/fs/cgroup/io.stat")
    io_totals = {}
    if io:
        for line in io.splitlines():
            for entry in line.split()[1:]:
                key, value = entry.split("=")
                io_totals[key] = io_totals.get(key, 0) + int(value)
    return {
        "monotonic_ns": time.monotonic_ns(),
        "cpu_stat": dict((k, int(v)) for k, v in (line.split() for line in cpu_stat.splitlines())) if cpu_stat else {},
        "proc_cpu": list(map(int, proc_stat.splitlines()[0].split()[1:])) if proc_stat else [],
        "context_switches": next((int(line.split()[1]) for line in (proc_stat or "").splitlines() if line.startswith("ctxt ")), None),
        "io_totals": io_totals,
        "memory_current": read("/sys/fs/cgroup/memory.current"),
        "cpu_pressure": read("/sys/fs/cgroup/cpu.pressure"),
        "io_pressure": read("/sys/fs/cgroup/io.pressure"),
        "driver_affinity": sorted(os.sched_getaffinity(0)),
    }


def difference(before, after):
    cpu = {k: after["cpu_stat"][k] - v for k, v in before["cpu_stat"].items() if k in after["cpu_stat"]}
    ticks = [b - a for a, b in zip(before["proc_cpu"], after["proc_cpu"])]
    total = sum(ticks[:8])  # guest counters are already included in user/nice.
    elapsed = (after["monotonic_ns"] - before["monotonic_ns"]) / 1e9
    return {
        "elapsed_seconds": round(elapsed, 6), "cgroup_cpu_delta": cpu,
        "cgroup_cpu_cores_used": round(cpu.get("usage_usec", 0) / (elapsed * 1e6), 4) if elapsed else None,
        "vm_steal_percent": round(ticks[7] * 100 / total, 4) if total and len(ticks) >= 8 else None,
        "vm_iowait_percent": round(ticks[4] * 100 / total, 4) if total and len(ticks) >= 5 else None,
        "io_delta": {k: after["io_totals"][k] - v for k, v in before["io_totals"].items() if k in after["io_totals"]},
        "before": before, "after": after,
        "scope": "entire VM/cgroup, including the workload; not per-engine resource attribution",
    }


def quiet_window(seconds=1.0, attempts=5):
    observations = []
    for _ in range(attempts):
        before = snapshot()
        time.sleep(seconds)
        observation = difference(before, snapshot())
        observations.append(observation)
        io_bytes = sum(observation["io_delta"].get(key, 0) for key in ("rbytes", "wbytes"))
        if (observation["cgroup_cpu_cores_used"] < 0.15
                and observation["cgroup_cpu_delta"].get("nr_throttled", 0) == 0
                and io_bytes < 2 * 1024**2
                and (observation["vm_iowait_percent"] or 0) < 2
                and (observation["vm_steal_percent"] or 0) < 2):
            return {"quiet": True, "observations": observations}
    return {"quiet": False, "observations": observations}
