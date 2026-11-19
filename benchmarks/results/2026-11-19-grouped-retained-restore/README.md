# Grouped synchronization for new-history retained restore

Retained restore checks the full live set against the output log, key count
and key-byte caps before reading values or appending records. It reuses the
compaction writer, which emits DB01 records in synchronized groups of at most
64 records and 4 MiB. Records larger than 4 MiB are synchronized alone. The
completed stage still reopens and checks every restored value against its
authenticated digest before publication.

The exact-limit test restores 130 records, observes both intermediate flushes,
and checks the final witness and all reopened values. Each one-under limit
rejects before invoking the reader or appending. The process-kill test
terminates the restore after the first group is synchronized and checks that
the destination is absent and the inputs remain intact. It also checks that a
destination made visible by rename reopens with the entire expected state.
The shared writer's separate fault tests cover partial append and sync errors.

The macOS ARM64 library suite passed 562 tests with Rust 1.100.0.
The Linux ARM64 library suite passed 724 with Rust 1.85.0 on both tmpfs
and ext4. Six retained integration tests and the updated crash test passed
separately on each platform. Formatting and warnings-denied Clippy passed.
The first ext4 run failed 24 subprocess launches with `ENOENT` while
another build reused the executable path. The successful ext4 run used a
byte-checked immutable copy of the final executable. The first run is
retained with the successful runs.
[macOS library log](macos-library.log.gz),
[Linux ext4 log](linux-ext4-verified.log.gz),
[Linux tmpfs log](grouped-restore-linux-tmpfs.log.gz),
[first Linux ext4 log](linux-ext4-first.log.gz),
[source hashes](source.sha256).

## Paired macOS sample

The existing release `retained_phase_bench` driver prepared 2,048 initial
keys with 1,024 suffix operations and seed 42, leaving 1,901 live keys.
Each timed process restored a fresh copy of the same fixture. The
baseline was `bb41efc`. Three alternating pairs included complete
restore, reopen verification and publication. Each output also matched
the driver’s independent full-state model, and the source, candidate,
suffix and binding file hashes matched across all samples.

| Implementation | Elapsed samples | Median | Median peak RSS |
| --- | --- | ---: | ---: |
| Per-record synchronization | 32.73 s, 25.04 s, 17.18 s | 25.04 s | 2.63 MiB |
| Group synchronization | 0.66 s, 0.66 s, 0.43 s | 0.66 s | 2.78 MiB |

The copy phase uses 30 synchronization groups for these small records.
The samples reflect a single warm fixture with concurrent validation
activity and do not establish a latency distribution or total memory
bound. Physical power-loss and independent off-host restore remain
deferred. [Timing, hashes and verification logs](macos-samples.tar.gz).

Reproduce with the [phase driver](../../../examples/retained_phase_bench.md).
Build each revision using `cargo build --offline --locked --release --example
retained_phase_bench`. Use `KEYS=2048 SUFFIX=1024 SEED=42` at setup, run
build, mutate and export, then copy the fixture for each restore sample.
Time the restore in a fresh process with `/usr/bin/time -l` and run the
driver’s verify phase outside the timed process.
