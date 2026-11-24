# Checkpoint batch preparation allocation probe

Base revision `0851ad4` and candidate `d04a678` each submitted a
4 MiB batch through the writable checkpoint store. One case used one
child and the other used 1,024 distinct children. The probe checked every
value immediately after the write and after a fresh reopen.

Four pairs ran on each host using Rust 1.85.0 release builds and
alternating execution order. Linux used VM tmpfs. Counts are heap allocation and reallocation
requests during the write. Setup and verification are outside both the
allocation count and timer. The timer includes log and witness
synchronization.

| Host | Children | Requests before → after | Requested bytes before → after | Median write µs before → after |
| --- | ---: | ---: | ---: | ---: |
| macOS ARM64 | 1 | 11 → 7 | 4,195,559 → 4,195,295 | 8,220.0 → 8,006.5 |
| macOS ARM64 | 1,024 | 2,338 → 1,303 | 4,575,938 → 4,461,434 | 8,061.5 → 8,645.0 |
| Linux ARM64 | 1 | 10 → 6 | 4,195,495 → 4,195,231 | 3,410.5 → 2,443.5 |
| Linux ARM64 | 1,024 | 2,337 → 1,302 | 4,575,874 → 4,461,370 | 4,078.5 → 3,236.0 |

The allocation totals repeated in all pairs. Elapsed time varied. The
1,024-child candidate took longer in all four macOS pairs. Linux had a
lower candidate median on its VM fixture. The
allocation counts describe requests in this probe. Process RSS and
filesystem allocation are outside the measurement.

The [raw archive](raw-artifacts.tar.gz) preserves 32 measured attempts and 16
discarded attempts, along with commands, build and source hashes, the driver,
and the source patch. The first Linux candidate build reused the baseline
binary, as shown by their matching hashes. The table uses only the
fresh-build Linux pairs, whose baseline and candidate hashes differ.
To reproduce, extract it, place an engine checkout at `0851ad4` in
`engine` alongside `driver`, and build with Rust 1.85.0 using
`cargo build --release --locked` inside `driver`. Save the binary,
apply `source.diff` in the engine checkout, and build a second binary in a
fresh target directory. Confirm the two binary hashes differ.
Execute both with argument `1` or `1024` on disposable fixture storage.
