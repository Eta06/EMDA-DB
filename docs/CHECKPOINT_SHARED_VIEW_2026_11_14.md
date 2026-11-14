# Sharing the validated checkpoint view's log handle

The writable V3 checkpoint store opens and validates its checkpoint view and
writer independently. After validation, it can let the read-only view share the
writer's log handle. The two logs must have the same V3 log identity, their
device and inode must match the actual shared handle, and the view must have
released its validation-only witness and anchor. If a file identity is missing,
an identity query fails, or the handles differ, the original independent view
is retained. The view's positional reads do not change the append offset; its
log API still rejects writes, synchronization and writer-witness verification.
Captured views retain the shared handle as long as they need it. V2 keeps the
existing independent path. The writer's witness and synchronization protocol,
on-disk format and recovery rules are unchanged.

## Resource measurements

The [first paired run](../benchmarks/results/2026-11-14-shared-checkpoint-view-pair.json.gz)
and [independent repeat](../benchmarks/results/2026-11-14-shared-checkpoint-view-pair-repeat.json.gz)
compare clean baseline `8c98f79` with clean candidate `89319db` on separate
100-store V3 fixtures, each with 1,000 keys per store. Each run alternated
baseline/candidate/candidate/baseline, with three fresh-process samples per
phase and mode. Each sample checked 300 point reads, and each fixture's log,
metadata witness, identity anchor and checkpoint matched before and after.
Descriptor counts include four process descriptors, including the descriptor
used to enumerate `/proc/self/fd`.

| Checkpoint-backed stores | Baseline first / last | Candidate first / last |
| --- | ---: | ---: |
| First run: median descriptors | 504 / 504 | 404 / 404 |
| First run: median total open time, ms | 31.25 / 69.76 | 32.70 / 36.34 |
| Repeat: median descriptors | 504 / 504 | 404 / 404 |
| Repeat: median total open time, ms | 29.07 / 30.33 | 28.20 / 28.44 |

The ordinary path used 404 descriptors in every phase. In these samples the
change saved one descriptor per V3 checkpoint store; it does not establish a
general startup-time improvement or a capacity limit. All 48 quiet-window
checks passed, but the shared VM still reported up to 5% steal and 8.34%
I/O wait during measurements. No cgroup throttling or process physical-read
bytes were observed in the samples. RSS is process-only and excludes the OS
page cache. The separately scoped [SQLite and multi-store comparison](MULTI_STORE_RESOURCES_2026_11_14.md)
does not compare SQLite against this candidate on the same fixture.

## Correctness evidence

- Stable debug, Rust 1.85 debug and stable release each passed 1,011 tests
  across 141 binaries on `89319db`; each ignored the existing guarded tmpfs
  ENOSPC test. Stable and minimum-version Clippy with warnings denied, the
  formatting check, and 147 Python harness tests also passed. The [evidence
  manifest](../benchmarks/results/2026-11-14-shared-checkpoint-view-evidence.json)
  links the full logs and their checksums.
- The guarded test was also run separately on `89319db` in an isolated
  8 MiB, 64-inode Docker tmpfs. It passed real kernel `ENOSPC` checks for V2
  and V3 writes, byte and inode exhaustion during backup, restore and upgrade,
  and initialization inode exhaustion. The [result](../benchmarks/results/2026-11-14-shared-checkpoint-view-enospc.json)
  records retries and preserved evidence. This does not emulate a physical
  disk or power failure.
- Focused tests cover a V3 captured view across a managed rollover and writer
  drop, read-only enforcement, V2 fallback, identity errors or unavailability,
  an unrelated shared handle, and restored logs with the same log identity on
  different inodes. The full suites include descriptor exhaustion and process
  termination tests.
- The [fault archive](../benchmarks/results/2026-11-14-shared-checkpoint-view-faults.tar.gz)
  contains independently verified reports for 360 V3 metadata `EIO`/`EAGAIN`
  injections and 94 V2/V3 `EIO` injections in selected sync, data and xattr
  reads, across checkpoint writes and rollover. Every injected case checked
  acknowledged data through both recovery orders and preserved reference
  evidence. Of these, 44 operations completed after an injected metadata
  error; the rest returned errors. Some errors occurred during opening rather
  than the later operation. The tests cover observed successful-path calls,
  not every filesystem failure or every failure-only path.
- AddressSanitizer runs of the checkpoint-store, checkpoint-exchange and
  automatic-checkpoint models completed 26,398 executions, followed by
  successful replay of all 1,155 saved corpus files. The [archive](../benchmarks/results/2026-11-14-shared-checkpoint-view-fuzz.tar.gz)
  includes the corpora and logs; the manifest records the binaries' hashes,
  seeds' run details and per-target counts. These are finite serial model runs,
  not a proof of all concurrent schedules.
- A later run on clean `f34f1d6` checked [96 concurrent transaction cases](../benchmarks/results/2026-11-14-shared-checkpoint-view-concurrent-eagain.json.gz)
  with injected `EAGAIN`, [224 short-write cases](../benchmarks/results/2026-11-14-shared-checkpoint-view-short-write.json.gz),
  and [48 interrupted-sync cases](../benchmarks/results/2026-11-14-shared-checkpoint-view-sync-interrupt.json.gz).
  Independent verifiers passed. The transaction run recovered all 159
  acknowledged pairs and found 96 of 384 failed calls present as complete
  uncertain pairs; the short-write run includes deliberately rejected recovery
  for partial records. These tests sample schedules and returned or intercepted
  syscall behavior, not physical power failures.

For example, extract the fault archive and run
`python3 scripts/verify_one_shot_fs_results.py <extracted-report>` for each
JSON report. Extract the fuzz archive and replay an individual corpus with
`cargo +nightly-2026-11-03 fuzz run checkpoint_store <corpus-path> -- -runs=507 -max_len=128 -timeout=15`.
The other retained corpora use `checkpoint_exchange` with `-runs=213 -max_len=64`
and `automatic_checkpoint` with `-runs=438 -max_len=128`.

These local checks do not establish physical power-loss safety, an independent
off-host restore, total project resource bounds, tenant isolation or
production readiness.
