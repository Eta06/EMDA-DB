# Empty-log recovery and reopen measurements

An existing zero-byte `records.log` can represent an empty legacy store, an interrupted new-store initialization, or lost data. These states cannot be distinguished from the bytes alone. Previously the ordinary open path treated the file as an unwitnessed legacy store. The [expected failing regression](../benchmarks/results/2026-11-04-empty-recovery-before-failure.txt) shows the earlier behavior at revision `a182778`; the new implementation rejects the ambiguous file without modifying it. A dangling witness link is also treated as an existing invalid witness instead of an absent one.

`Store::upgrade_empty_legacy` explicitly copies an **independently verified** empty unwitnessed legacy log to a new witnessed store without changing the source log. The caller must first establish that the file is really intended to be empty; this API cannot recover lost data. Restoring a validated zero-length legacy backup now produces an empty witnessed store with a new identity. New initialization continues to use V3 on supported filesystems and V2 when metadata witnesses are unsupported. These are observable changes to the unreleased format behavior; see [versioning](VERSIONING.md) and the [storage contract](STORAGE_TRANSACTION_CONTRACT.md).

## Local validation

- Stable and Rust 1.85 formatting, clippy and all-target tests passed on the recovery implementation. After strengthening the tests against transient inherited locks, 15 consecutive parallel release library suites passed at revision `41b375e`. The earlier repeated runs exposed lock-sensitive test failures; those failures were investigated and the tests still require the expected corruption or verification error after a temporary lock clears.
- Initialization tests terminate a child at five boundaries; restore tests inject failures and terminate children at new empty-restore boundaries. A separate [80-run stress record](../benchmarks/results/2026-11-04-empty-recovery-stress.json) repeats eight targeted tests five times each on `/tmp` tmpfs and the workspace overlay. The [ad hoc driver](../benchmarks/results/2026-11-04-empty-recovery-stress-driver.py) is retained with its original paths; the record contains test-binary hashes, source revision and mount types.
- Four [coverage-guided campaigns](../benchmarks/results/2026-11-04-empty-recovery-fuzz.json) exercised 213,315 inputs: 152,110 recovery, 42,006 backup/restore, 15,301 checkpoint recovery and 3,898 managed checkpoint inputs. They ran against revision `ddaae07`; later revisions changed test fixtures, not the production recovery code. The record includes seeds and binary hashes. Finite fuzzing is not exhaustive.

## Warm-cache reopen comparison

The [driver](../scripts/recovery_before_after.py) prepared each witnessed fixture once using the baseline and verified identical log, witness and metadata contents before and after each measurement. Both revisions used the same benchmark source and Cargo lockfile, separately built release binaries and the same files. Each of six processes per variant and case reopened the fixture 50 times, verifying sampled values after each reopen. Pass order was baseline/candidate/candidate/baseline; the table is the median of the six process p50s and p95s in microseconds, not a pooled percentile. No new-store creation or competitor engine was measured.

| Filesystem and run | Records | Baseline p50 / p95 (µs) | Candidate p50 / p95 (µs) |
| --- | ---: | ---: | ---: |
| tmpfs, first | 1 | 86.5 / 201 | 97 / 165 |
| tmpfs, repeat | 1 | 93 / 163.5 | 94 / 185 |
| tmpfs, first | 1,000 | 340.5 / 446.5 | 370 / 503.5 |
| tmpfs, repeat | 1,000 | 362.5 / 455 | 364.5 / 496.5 |
| workspace overlay | 1 | 408 / 715 | 421 / 567.5 |
| workspace overlay | 1,000 | 681 / 1,047 | 697 / 951 |

The first tmpfs run sampled higher candidate medians, while the repeat was much closer; overlay medians were also higher for the candidate. Tails were mixed. Integer-microsecond timers, short runs and VM interference limit the conclusion. CPU affinity and quiet windows were used; one first-run quiet window failed and individual short samples reported up to 11.1% VM steal. The overlay's backing device is unknown. Raw samples, exact revisions, binary and driver hashes, and cgroup/VM telemetry are in the [first](../benchmarks/results/2026-11-04-empty-reopen-primary.json.gz), [repeat](../benchmarks/results/2026-11-04-empty-reopen-repeat.json.gz) and [overlay](../benchmarks/results/2026-11-04-empty-reopen-overlay.json.gz) records.

These checks cover process termination while the OS is running, not physical power failure. Neither filesystem is an independent off-host restore target. Those two drills remain explicitly deferred in the [foundation checklist](FOUNDATION_FOLLOW_UP.md).
