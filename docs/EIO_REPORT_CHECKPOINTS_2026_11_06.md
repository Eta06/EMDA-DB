# Paired-EIO report checkpoints — 6 November 2026

The paired-failure runner now writes a small current-case sidecar and
periodically snapshots its full report. It forces a full report on normal
completion or a caught error. After an abrupt termination, the main report can
lag behind the sidecar; both should be retained. `--snapshot-interval 0`
requests a full snapshot at each driver update.

On clean revision `8acec7d7ae742f0f79b8dde5598fdb070479e7f9`, four local runs
selected the same V3 checkpoint-compaction workload: 26 primary probes and
208 paired-error cases per run. The zero-interval runs each wrote 470 full
report snapshots; the one-second runs each wrote 12. All four completed with
identical selected fault ordinals and recovery orders. A separate driver
SIGKILL check parsed both retained files and confirmed that the sidecar
recorded the later active paired case while the full report remained valid.
Unit tests also check forced failure snapshots and preservation of an earlier
report when serialization fails.

The [summary](../benchmarks/results/2026-11-06-eio-report-interval.json) and
[raw archive](../benchmarks/results/2026-11-06-eio-report-interval.tar.gz)
retain the run details and interruption check. The zero-interval runs took
29.33 and 24.50 seconds; the one-second runs took 11.08 and 11.14 seconds on
the shared VM while another fault campaign ran. These are exploratory
measurements of the **test runner**, not database operation latency or a
guarantee of speedup elsewhere. The report files are process-level evidence;
this does not test power-loss durability.

The [hosted filesystem workflow](https://github.com/Eta06/EMDA-DB/actions/runs/42165362009)
passed the updated runner and independent report verifier on ext4. Its artifact
includes both completed reports and their sidecars; it recorded 28 full
snapshots for 998 paired cases and eight for 218 ancestor-sync cases.

## One-shot campaign progress

The one-shot and persistent-error driver now uses the same approach: it
records the current case in a small `.progress.json` sidecar and writes full
reports at a configurable interval, forcing the full result on completion or
a caught error. A completed sidecar is published only after its corresponding
full result. `--snapshot-interval 0` retains a full snapshot at each update;
the default interval is one second. Both output paths must be unused.

An eight-injection local metadata pilot with a 60-second interval recorded two
full snapshots after the initial report creation and a completed sidecar. A
persistent-error pilot also completed, and a forced probe failure retained its
error in both files. In a separate SIGKILL check, a disposable driver and its
process group were stopped while a case was active; both retained reports
remained incomplete and the sidecar identified the last active case. These
checks concern the **test runner**, not database throughput or physical
power-loss durability of the report files.
