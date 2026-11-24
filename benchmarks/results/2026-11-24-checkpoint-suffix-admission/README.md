# Checkpoint suffix admission

The probe compared `83715b0` with the suffix admission patch on macOS
ARM64. Each run wrote a 4 MiB batch to a writable checkpoint store and
verified every child before and after reopen. Four pairs ran per batch
size with alternating execution order and Rust 1.85.0 release builds.

| Children | Allocation requests before → after | Requested bytes before → after | Median write µs before → after |
| ---: | ---: | ---: | ---: |
| 1 | 7 → 5 | 4,195,295 → 4,195,039 | 7,644 → 7,807.5 |
| 1,024 | 1,303 → 1,199 | 4,461,434 → 4,409,626 | 8,065 → 8,172 |

The allocation counts repeated across all pairs. Timings include durable
append and witness synchronization and overlap across variants. This
sample establishes the allocation reduction on the probe. The timed
workload does not trigger rollover or measure process RSS.

The [raw archive](raw-artifacts.tar.gz) contains all 16 attempts, build
logs, distinct binary hashes, the patch, and the probe with its lockfile.
To reproduce, place the baseline checkout in `engine` alongside
`driver`, build with `cargo +1.85.0 build --release --locked` in
`driver`, then apply `source.diff` and build in a separate target
directory. Run each binary on disposable storage with argument `1` or
`1024`. The archive includes the Linux ARM64 focused test log for the
patch. Timed pairs were run on the Mac.

The [hosted archive](hosted-cross-engine.tar.gz) holds all three successful
cross-engine smoke cases from baseline revision `83715b0`. It preserves
the compound range, crash and document contention commands and raw
outputs. These use smoke workloads. [Hosted run](https://github.com/Eta06/EMDA-DB/actions/runs/45331937512).
