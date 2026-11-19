# Trusted spill run key bounds

The spill writer checks strict key order and retains the first and last
key only after verifying its output. Replay skips a run when the sought
key falls outside those bounds. A run with a tombstone at either endpoint
still participates in lookup. The bounds add at most two keys of logical
storage per run, with fan-in capped at 32, plus a writer under construction.
Disk format, entry checksums and whole-run digest checks are unchanged.
The regression mutates a key outside the bounds, checks ordered accounting
against the original base, then verifies the run is rejected before emission.

## Large-suffix comparison

The workload is the exact [large-suffix rebuild test](../../../tests/bounded_rebuild_large_suffix.rs)
with 16,385 suffix keys, 4,096 keys per chunk and fan-in of four. Both
binaries were built in the same Linux ARM64 VM with Rust 1.85.0 and test
debug symbols and incremental compilation disabled. The baseline is
`e4d1419`, whose binary and trace were retained from the
[exact-span measurement](../2026-11-19-spill-entry-spans/README.md).
Six pairs of untraced single-test invocations alternate order on tmpfs.
The elapsed time includes fixture setup, rebuild and state verification.
Separately traced runs count positive or zero results from `pread64` on
run files, excluding offset sidecars.

| Metric | Baseline | Key bounds |
| --- | ---: | ---: |
| Completed run reads | 979,314 | 232,266 |
| Logical run bytes read | 28,617,300 | 10,888,644 |
| Median elapsed seconds | 0.995 | 0.570 |
| Elapsed range in seconds | 0.98–1.03 | 0.57–0.63 |

[Read totals](linux-spill-preads.txt), [paired runs and binary hashes](linux-timing.tar.gz),
[trace hashes](linux-traces.sha256), [environment](linux-environment.log).
These results reflect this workload and VM. Overlapping runs can still
require a lookup in each run. This evidence does not give a total memory,
elapsed time or physical storage traffic bound.

## Correctness validation

The matching [source manifest](linux-source.sha256) covers Rust source and
tests. The focused integration tests cover bounded managed rebuild,
the larger-than-overlay suffix and the seeded full-log differential model.

| Platform | Library tests at 16 threads | Targeted rebuild integration tests |
| --- | ---: | ---: |
| macOS ARM64 | 575 passed | 9 passed |
| Linux ARM64 | 737 passed | 10 passed |

Mac formatting and all-target Clippy with warnings denied also passed.
[Mac tests](macos-tests.log.gz), [Linux library](linux-library.log.gz),
[Linux integration](linux-integration.log.gz).

The [spill fault runner](../2026-11-19-bounded-spill-io-faults/README.md)
exercised V2 and V3 with acknowledged and unacknowledged complete
suffixes. Each of the four fixtures had 590 pre-publication spill
syscalls selected for injection, compared with 643 before these key
bounds. The runner verifies values and checkpoint bytes against
full-log replay after every completed or retried rebuild.

| Filesystem | Injected error | Cases | Published after syscall retry | Cleanly rejected |
| --- | --- | ---: | ---: | ---: |
| ext4 | EIO | 2,360 | 0 | 2,360 |
| ext4 | EINTR | 2,360 | 1,876 | 484 |
| tmpfs | EIO | 2,360 | 0 | 2,360 |
| tmpfs | EINTR | 2,360 | 1,876 | 484 |

Each rejection preserved the prior selection and source bytes. In 24 EIO
cases on each filesystem, the witness had advanced to the exact validated
tail before the later spill failure. The runner checked the initial or
validated tail witness and a clean retry in every rejection case.
[Fault records, controls and probe hash](linux-faults.tar.gz).
Each injection targets one syscall. Physical power loss and independent
off-host restore remain deferred.
