#!/usr/bin/env python3
"""Run strictly interleaved baseline (d8254003) vs candidate benchmarks."""

from __future__ import annotations

import json
import os
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path


def run_cmd(cmd: list[str], cwd: str | None = None) -> tuple[int, str, str, float]:
    t0 = time.monotonic()
    proc = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    dt = time.monotonic() - t0
    return proc.returncode, proc.stdout, proc.stderr, dt


def parse_json_lines(stdout: str) -> list[dict]:
    out = []
    for line in stdout.splitlines():
        line = line.strip()
        if line.startswith("{") and line.endswith("}"):
            out.append(json.loads(line))
    return out


def summarize_metric(values: list[float]) -> dict:
    return {
        "values": values,
        "min": round(min(values), 1),
        "median": round(statistics.median(values), 1),
        "max": round(max(values), 1),
    }


def run_sql_bench_suite(
    out_dir: Path,
    base_bin: str,
    cand_bin: str,
    rows: int,
    ops: int,
    trials: int,
) -> dict:
    log_path = out_dir / f"sql_bench_{rows}.log"
    records: dict[str, list[list[dict]]] = {"baseline": [], "candidate": []}
    with log_path.open("w") as log:
        for trial in range(1, trials + 1):
            order = (
                [("baseline", base_bin), ("candidate", cand_bin)]
                if trial % 2 == 1
                else [("candidate", cand_bin), ("baseline", base_bin)]
            )
            for label, binary in order:
                tmp = tempfile.mkdtemp(prefix=f"emda-sql-{rows}-{label}-t{trial}-")
                try:
                    rc, stdout, stderr, dt = run_cmd([binary, tmp, str(rows), str(ops)])
                finally:
                    shutil.rmtree(tmp, ignore_errors=True)
                log.write(
                    f"=== trial={trial} build={label} rows={rows} ops={ops} rc={rc} elapsed_s={dt:.2f} ===\n"
                )
                log.write(stdout)
                if stderr:
                    log.write(f"--- stderr ---\n{stderr}\n")
                log.flush()
                if rc != 0:
                    raise RuntimeError(
                        f"sql_bench failed (trial={trial}, build={label}, rows={rows}, rc={rc}):\n{stderr}"
                    )
                records[label].append(parse_json_lines(stdout))

    summary: dict[str, dict] = {}
    for build in ("baseline", "candidate"):
        by_backend: dict[str, list[dict]] = {}
        for run_rows in records[build]:
            for row in run_rows:
                by_backend.setdefault(row["backend"], []).append(row)
        build_summary: dict[str, dict] = {}
        for backend, runs in by_backend.items():
            if any("skipped" in r for r in runs):
                build_summary[backend] = {"skipped": runs[0]["skipped"]}
                continue
            b: dict[str, dict] = {}
            for key in ("load_ms",):
                if key in runs[0]:
                    b[key] = summarize_metric([r[key] for r in runs])
            for metric in (
                "parse",
                "pk_select",
                "insert",
                "pk_update",
                "pk_delete",
                "full_select",
                "get",
                "delete",
                "idx_eq",
                "idx_range_10",
                "insert_1idx",
                "insert_3idx",
            ):
                if metric in runs[0] and isinstance(runs[0][metric], dict):
                    b[metric] = {
                        stat: summarize_metric([r[metric][stat] for r in runs])
                        for stat in ("p50_us", "p99_us", "max_us")
                    }
            build_summary[backend] = b
        summary[build] = build_summary
    return summary


def run_probe_suite(
    out_dir: Path,
    base_bin: str,
    cand_bin: str,
    rows: int,
    ops: int,
    trials: int,
) -> dict:
    log_path = out_dir / f"checkpoint_read_probe_{rows}.log"
    records: dict[str, list[list[dict]]] = {"baseline": [], "candidate": []}
    with log_path.open("w") as log:
        for trial in range(1, trials + 1):
            order = (
                [("baseline", base_bin), ("candidate", cand_bin)]
                if trial % 2 == 1
                else [("candidate", cand_bin), ("baseline", base_bin)]
            )
            for label, binary in order:
                tmp = tempfile.mkdtemp(prefix=f"emda-probe-{rows}-{label}-t{trial}-")
                try:
                    rc, stdout, stderr, dt = run_cmd([binary, tmp, str(rows), str(ops)])
                finally:
                    shutil.rmtree(tmp, ignore_errors=True)
                log.write(
                    f"=== trial={trial} build={label} rows={rows} ops={ops} rc={rc} elapsed_s={dt:.2f} ===\n"
                )
                log.write(stdout)
                if stderr:
                    log.write(f"--- stderr ---\n{stderr}\n")
                log.flush()
                if rc != 0:
                    raise RuntimeError(
                        f"checkpoint_read_probe failed (trial={trial}, build={label}, rc={rc}):\n{stderr}"
                    )
                records[label].append(parse_json_lines(stdout))

    summary: dict[str, dict] = {}
    for build in ("baseline", "candidate"):
        grouped: dict[str, list[dict]] = {}
        for run_rows in records[build]:
            for row in run_rows:
                tag = row["phase"] + (f":{row['label']}" if "label" in row else "")
                grouped.setdefault(tag, []).append(row)
        build_summary: dict[str, dict] = {}
        for tag, runs in grouped.items():
            entry: dict[str, object] = {}
            for k, v in runs[0].items():
                if k in ("phase", "label"):
                    continue
                if isinstance(v, dict) and "p50_us" in v:
                    entry[k] = {
                        stat: summarize_metric([r[k][stat] for r in runs])
                        for stat in ("avg_us", "p50_us", "p99_us", "max_us")
                    }
                elif isinstance(v, (int, float)) and not isinstance(v, bool):
                    entry[k] = summarize_metric([float(r[k]) for r in runs])
            build_summary[tag] = entry
        summary[build] = build_summary
    return summary


def main() -> int:
    if len(sys.argv) != 6:
        print(
            "usage: run_interleaved_bench.py <out_dir> <base_sql_bench> <cand_sql_bench> <base_probe> <cand_probe>",
            file=sys.stderr,
        )
        return 2
    out_dir = Path(sys.argv[1])
    out_dir.mkdir(parents=True, exist_ok=True)
    base_sql, cand_sql, base_probe, cand_probe = sys.argv[2:6]
    trials = 3

    summary = {
        "trials": trials,
        "sql_bench_10000_100": run_sql_bench_suite(
            out_dir, base_sql, cand_sql, 10000, 100, trials
        ),
        "sql_bench_2000_100": run_sql_bench_suite(
            out_dir, base_sql, cand_sql, 2000, 100, trials
        ),
        "checkpoint_read_probe_10000_100": run_probe_suite(
            out_dir, base_probe, cand_probe, 10000, 100, trials
        ),
    }
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(f"Wrote {out_dir / 'summary.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
