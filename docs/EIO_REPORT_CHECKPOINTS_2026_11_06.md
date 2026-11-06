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
