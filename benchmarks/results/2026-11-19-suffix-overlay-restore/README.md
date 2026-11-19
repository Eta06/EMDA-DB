# Retained restore with suffix-touched keys

Restore tracks locations only for keys mentioned in the detached suffix.
It authenticates the suffix, identifies those keys, and scans the base
to establish their original lengths. Ordered replay then checks every
live-set transition using the same entry, key-byte, value, payload and
work admission rules. It admits the exact output count and size before
append and checks those totals while streaming into the private store.

A second authenticated base scan merges unchanged rows with the final
suffix entries. Deleted or replaced base rows still participate in base
authentication. The streaming verifier compares the complete witnessed
log with the digest of the ordered emitted keys, lengths and value digests.
The corruption regression covers a changed live base value, a changed
deleted base value, a changed suffix value and tampered staged output.
The exact-limit and process-kill tests exercise output admission and
publication, including termination after a synchronized group.

The 16-thread library suites passed 562 tests on macOS ARM64 with Rust
1.100.0 and 724 on Linux ARM64 ext4 with Rust 1.85.0. Both used fixed copies
of the built binaries. Six retained integration tests passed separately
on each platform. Formatting and warnings-denied Clippy passed on
macOS. The first Linux test compiler received SIGKILL during a concurrent
benchmark build. The serialized build and test run succeeded after
discarding the verified synthetic output copies and benchmark build cache.
[macOS suite](macos-library.log.gz), [Linux suite](linux-library.log.gz),
[macOS integration](macos-integration.log.gz),
[Linux integration](linux-integration.log.gz), [source hashes](source.sha256).

Restore memory depends on distinct suffix keys, including tombstones,
the copy groups and the largest value. Suffix bytes and operation limits
bound the input from which those keys are allocated. This shifts the
memory cost to the suffix for workloads with many distinct, short-lived
keys. It also performs another verified pass over each input. The
existing work limit remains an admission formula. These limits and the
sample below do not establish a total-memory or latency guarantee.

## Linux ARM64 many-key sample

The [previous fixture](../2026-11-19-streamed-restore-verification/README.md)
has 400,000 initial keys and 1,024 suffix operations. The final live set
contains 399,853 keys. The baseline is the hash-checked `32ec6b0` executable
from that comparison. Each pair uses the same warm tmpfs fixture and
fresh processes. All six outputs independently reopened as ordinary
stores and matched the full-state model. The base, suffix, source and
binding file hashes matched before and after the measurements.

| Restore implementation | Median elapsed | Median peak RSS |
| --- | ---: | ---: |
| Full live-key location index | 0.24 s | 46.65 MiB |
| Suffix-touched keys and streaming base | 0.19 s | 4.29 MiB |

Both timed operations include replay, staged writes, verification and
no-clobber publication. The measurement describes a sparse suffix on a
single host and excludes preparation and independent model comparison.
[Build, binary hashes and three paired samples](linux-samples.tar.gz).
Reproduce with the previous fixture patch and the same separate-target
build and sampling procedure.
