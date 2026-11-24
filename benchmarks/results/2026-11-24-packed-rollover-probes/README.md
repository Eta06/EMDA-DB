# Managed checkpoint replay-probe allocations

The [probe](rollover_alloc_probe.rs.txt) seeded 16,384 base keys, then applied
one of three suffix sizes with updates, deletes, inserts and absent-key
tombstones. Each invocation created a fresh fixture on Apple Silicon macOS,
timed managed rollover, counted allocation and reallocation requests in the
rollover call, reopened the managed store, and checked every expected key.
Both binaries were built with Rust 1.85.0 in release mode. Baseline revision
was `44db933` and the candidate diff is in [provenance.txt](provenance.txt).

| Touched keys | Allocation calls before → after | Allocation bytes requested before → after |
|---:|---:|---:|
| 0 | 66 → 66 | 94,346 → 94,346 |
| 64 | 142 → 135 | 191,244 → 186,724 |
| 4,096 | 4,550 → 4,174 | 1,677,874 → 1,208,330 |

The allocation totals repeated across all four samples per binary and suffix
size. Reallocation size is counted as requested bytes. These figures do not
measure peak memory or OS page cache. Timing was noisy in this single-host
run. No cross-engine performance conclusion follows. Separately initialized
fixtures have different log identities and checkpoint digests.

[Accepted raw samples](samples.log) use independent Cargo target directories
and identified binaries. The initial [shared-target attempt](invalid-shared-target-samples.log)
is preserved and excluded because Cargo reused the baseline binary.
