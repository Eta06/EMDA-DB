# Linux ARM64 validation — 2026-11-15

These results cover an uncommitted working-tree snapshot based on `a2f6192`,
not that commit alone. The [source manifest](../benchmarks/results/2026-11-15-linux-arm64-source-manifest.json)
records the snapshot's file hashes and archive SHA-256. The archive itself is
in temporary local storage; the manifest is not a replacement for a commit.

The environment was Ubuntu 26.10 ARM64, kernel `7.3.0-9-generic`, in a Lima
VZ virtual machine on Apple silicon. Tests ran on its ext4 filesystem with
user xattrs available. This is a VM and does not establish physical power-loss
behavior, physical-disk ENOSPC behavior, an off-host restore, or x86-64 results.

- Stable Rust 1.100.0: 1,051 all-target tests passed; one disposable-tmpfs
  test was ignored. Formatting, all-target Clippy and 150 Python tests passed.
  [Raw log](../benchmarks/results/2026-11-15-linux-arm64-worktree-stable-tests.log.gz).
- Rust 1.85.0: 1,051 all-target tests passed; the same test was ignored.
  Formatting and all-target Clippy passed.
  [Raw log](../benchmarks/results/2026-11-15-linux-arm64-worktree-185-tests.log.gz).
- Stable optimized: 1,051 all-target tests passed; the same test was ignored.
  [Raw log](../benchmarks/results/2026-11-15-linux-arm64-worktree-release-tests.log.gz).
  All three included the improved rebuild assertion, which passed; the older
  unrecorded failure remains unexplained.
- The separately guarded [disposable tmpfs ENOSPC test](../benchmarks/results/2026-11-15-linux-arm64-worktree-enospc.json)
  passed inside the pinned Docker container. It exercised byte and inode
  exhaustion, including V2/V3 writes and copy/retry paths.
- The staging cleanup syscall driver and its verifier completed 30 injected
  calls across EIO, EAGAIN, EINTR, ENOSPC and EDQUOT, using sync and namespace
  groups with at most three ordinals each. Three sync EINTR injections were
  handled successfully; the others reported uncertain outcomes. The
  [reports](../benchmarks/results/2026-11-15-linux-arm64-staging-faults.tar.gz)
  describe synthetic returned errors and do not cover every call or path.
- With nightly `2026-11-03` and cargo-fuzz 0.13.2, 61-second campaigns completed
  292,087 staging-cleanup inputs and 502,358 recovery inputs without a finding.
  [Log](../benchmarks/results/2026-11-15-linux-arm64-fuzz.log.gz) and
  [corpora](../benchmarks/results/2026-11-15-linux-arm64-fuzz-corpus.tar.gz)
  are retained. A [subsequent replay](../benchmarks/results/2026-11-15-linux-arm64-fuzz-replay.log.gz)
  loaded 153 staging and 76 recovery corpus files without a finding. The
  recovery target caps input at 8 KiB and does not exercise
  the multi-buffer replay path; targeted large-record tests cover selected
  cases, not all possible faults.
- A later, separately added large-record target generated 65,537–65,600-byte
  values from bounded control inputs on legacy, V2 and default witnessed
  stores. A 61-second instrumented run completed 91,150 inputs without a
  finding; [log](../benchmarks/results/2026-11-15-linux-arm64-large-record-fuzz.log.gz),
  [corpus](../benchmarks/results/2026-11-15-linux-arm64-large-record-fuzz-corpus.tar.gz),
  and [replay log](../benchmarks/results/2026-11-15-linux-arm64-large-record-fuzz-replay.log.gz)
  are retained. Replay loaded 61 corpus files. This target and its fuzz
  manifest were added after the source snapshot: the target SHA-256 was
  `882bbe7fb9a8ecdb25aa84212e7eb9937bf19ef30b0b345324e06f5152b9dd04`,
  `fuzz/Cargo.toml` was `e52fade5d0abbb39c13395e5c9992a1ab91be839cde5afc5084f90d32dcbc1de`,
  and `fuzz/Cargo.lock` was `456428b821c43b36f1530cc69479fef72c5eec62c06e3d3388637632920e5228`.
  The production log and recovery sources matched the earlier snapshot. This
  finite generated campaign does not establish power-loss behavior.

## Streamed atomic-batch replay

A later uncommitted snapshot added the streaming DB02 parser and tests. Its
[file manifest](../benchmarks/results/2026-11-15-linux-arm64-batch-final-manifest.json)
identifies the tested source separately from the earlier snapshot. Stable,
Rust 1.85 and optimized stable all-target runs each passed **1,061 tests**, with
one ignored disposable-tmpfs test. Stable and 1.85 formatting and all-target
Clippy passed; stable also passed 150 Python tests. Logs: [stable](../benchmarks/results/2026-11-15-linux-arm64-batch-stable-tests.log.gz),
[1.85](../benchmarks/results/2026-11-15-linux-arm64-batch-185-tests.log.gz),
and [optimized](../benchmarks/results/2026-11-15-linux-arm64-batch-release-tests.log.gz).
The improved rebuild test passed in each run; the historical failure remains
unexplained.

The first [workflow check](../benchmarks/results/2026-11-15-linux-arm64-batch-workflow-failure.log.gz)
failed because the newly added fuzz target unbalanced the shards. The target
was moved to shard 4 before the passing snapshot above. Targeted V2/V3 tests
additionally checked adoption and rejection of large unacknowledged batches,
including witness preservation and ordered quotas, and replay of a maximum
16 MiB encoded batch. [Targeted logs](../benchmarks/results/2026-11-15-linux-arm64-batch-replay-tests.tar.gz)
include earlier targeted runs; the full-suite logs cover the final tests.

The guarded [tmpfs ENOSPC test](../benchmarks/results/2026-11-15-linux-arm64-batch-enospc.json)
passed again on this decoder. A [second staging cleanup campaign](../benchmarks/results/2026-11-15-linux-arm64-batch-staging-faults.tar.gz)
independently verified 30 selected synthetic syscall injections, with the same
five error codes and limits as above.

After that snapshot, the large-record fuzz target was extended to generate
DB02 batches and repair the outer CRC after corrupting nested bytes. The
61-second instrumented run completed 85,331 inputs without a finding; its
[log](../benchmarks/results/2026-11-15-linux-arm64-batch-fuzz.log.gz),
[corpus](../benchmarks/results/2026-11-15-linux-arm64-batch-fuzz-corpus.tar.gz)
and [replay](../benchmarks/results/2026-11-15-linux-arm64-batch-fuzz-replay.log.gz)
are retained. Replay loaded 71 files. The later fuzz target SHA-256 was
`0e9d41fb38c6a7b32ecda58709e15d5f2f134d00fa04ca318bf779363f85700f`;
the production `src/log.rs` and `src/recovery.rs` matched the batch snapshot.
These finite campaigns and VM tests do not establish physical power-loss
behavior or a total-memory or bounded-recovery-time guarantee.

An additional [rebuild lock test](../benchmarks/results/2026-11-15-linux-arm64-rebuild-lock-tests.log.gz)
passed on stable and 1.85. A duplicated descriptor retained an exclusive lock
after the original descriptor closed; rebuild reported `NotPublished` with
`WouldBlock`, preserved the selection, and succeeded after the duplicate was
closed. This models an inherited open-file description, but does not establish
the cause of the earlier failure. The later test file SHA-256 was
`5c090a99d123cfe5ab01727ab3e72eb9f65814f9f9164cdbc2d0ea1f9f7bd48d`;
the rebuild implementation used was the batch-validation snapshot above.

A subsequent stable library run with 16 test threads stopped after its first
round: 513 passed and three failed. The preserved [failure log](../benchmarks/results/2026-11-15-linux-arm64-rebuild-stress-failure.log.gz)
has SHA-256 `3ab5ef27434b04c125cfaebd5c044dd6d4d9934bc000f2786a0e22f1b4b3f41c`.
The publication-boundary test received `NotPublished` with OS error 11
(`WouldBlock`) before its checkpoint-sync injection. A checkpoint reader test
also received `WouldBlock` on reopen; a second rebuild test observed a
disconnected worker before its synchronization hook. The latter worker's
actual return value was not captured. These results are consistent with lock
contention, but do not prove the cause of the historical failure or of every
failure in this run. No later rounds were run after the first failure.

On November 16, a later snapshot with the bounded rebuild and the document
rename correction passed Linux ARM64 stable all-target Clippy and the full
[all-target test run](../benchmarks/results/2026-11-16-linux-arm64-bounded-tests.log.gz):
1,075 passed, none failed, and one was ignored. The
[Clippy log](../benchmarks/results/2026-11-16-linux-arm64-bounded-clippy.log.gz)
is retained. This snapshot adds the bounded rebuild and its tests to the prior
batch snapshot; the changed production files had SHA-256 values `5a1a0e62864bb39ab251886c1c4a1d2599a143e3f7c27a9ec5a1006034b4d27e`
(`src/log.rs`), `31b62872881c3ba9f52b6ccb57213b909898f3ed7c1ee2ad3142edbe51c2ff7a`
(`src/checkpoint_store.rs`), `235be4cf2e6a822c19d7331dc545b057be38bfea77f4e97758e68b196cf63ad3`
(`src/checkpoint_store/rebuild.rs`), `ee6074bb2c0d72c2be098b9f23d09a012cf29de0a7cffa9d9355253557545020`
(`src/checkpoint_store/rebuild_spill.rs`), and `66cb68d9514a1171dd8679ccbaeac425bc73a3ba06457f046f8ec32ff3b88d51`
(`src/document/update/scalar.rs`). This passing run does not explain or erase
the earlier 16-thread failure.

Separate stable release model runs on the same VM requested 4,096 ordinary
transaction histories and 4,096 checkpoint transaction histories via their
respective environment variables. Both test binaries passed; the
[ordinary log](../benchmarks/results/2026-11-16-linux-arm64-ordinary-history.log.gz)
and [checkpoint log](../benchmarks/results/2026-11-16-linux-arm64-checkpoint-history.log.gz)
are retained. These finite model schedules and clean reopens do not cover all
interleavings, injected storage failures, or physical power loss.

After the array-truncation changes, Rust 1.85 all-target Clippy passed. The
first all-target test build stopped because the VM filesystem filled during
linking; its [failure log](../benchmarks/results/2026-11-16-linux-arm64-185-space-failure.log.gz)
is retained. After removing only generated debug and release build outputs,
the [test rerun](../benchmarks/results/2026-11-16-linux-arm64-185-current-tests.log.gz)
passed 1,084 tests, with one ignored. These results do not resolve the earlier
parallel lock failures.

A later parallel Rust 1.85 targeted rebuild run had 13 passes and one
failure: the injected `EntryTag` case received `NotPublished(WouldBlock)`
before its corruption hook. A separately run single-threaded control passed
all 14 targeted rebuild tests, and the bounded-rebuild model and large-suffix
tests passed. The control does not identify the lock holder or resolve the
parallel failure.

A subsequent Rust 1.85 Linux library run with 16 test threads under `strace`
passed all 532 tests. Its [syscall trace and test log](../benchmarks/results/2026-11-16-linux-arm64-lock-strace.tar.gz)
are retained. Because the assertion did not fail in that run, the trace does
not identify the earlier holder or establish a root cause. Test-only diagnostics
were subsequently added to report the lock inode and matching `/proc/locks`
entries on `WouldBlock`, without retrying or changing the returned error.

The later spill-format integrity fix passed a Linux ARM64 Rust 1.85
[all-target run](../benchmarks/results/2026-11-16-linux-arm64-spill-v2-185-tests.log.gz):
1,095 passed, none failed, and one was ignored. That run preceded a further
test-only byte-corruption campaign. It does not resolve the parallel lock
failure recorded above.
