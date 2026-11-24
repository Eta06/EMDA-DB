# Tuple encoding and relationship lookup

Filtered unique-tuple lookup with buffer reuse at `bf099f5` was
paired with `1a827c3` on macOS ARM64 APFS and Linux ARM64 VM tmpfs. Both
release binaries use Rust 1.85.0. Each query returned 128 source rows
against 512 target rows and independently verified 87 matches and 41
misses. SQLite 3.49.1 used the same partial unique join and served as a
timing control in every process.

Each host ran four pairs with alternating execution order. Every process
ran four warm-up rounds followed by 20 measured rounds per engine,
with the first timed engine alternating by round. The table reports
median microseconds for a complete response including JSON materialization.
Fixture setup and result comparison ran outside the timer.

| Host | Pair | Prior EMDA | Buffered EMDA | SQLite with prior build | SQLite with buffered build |
| --- | ---: | ---: | ---: | ---: | ---: |
| macOS | 1 | 263.5 | 214.1 | 83.1 | 80.8 |
| macOS | 2 | 326.2 | 279.6 | 106.6 | 106.0 |
| macOS | 3 | 346.8 | 282.2 | 110.3 | 109.5 |
| macOS | 4 | 198.4 | 166.3 | 61.7 | 64.8 |
| Linux | 1 | 230.5 | 219.8 | 77.9 | 96.2 |
| Linux | 2 | 334.1 | 197.9 | 109.4 | 62.3 |
| Linux | 3 | 343.5 | 311.5 | 117.1 | 125.3 |
| Linux | 4 | 318.4 | 295.0 | 122.3 | 118.4 |

The candidate median was lower in every pair. Linux pair 2 also saw
a large change in the SQLite control. The other Linux pairs show a
smaller reduction. SQLite had the lower median on every run of this
workload.

The [raw archive](raw-artifacts.tar.gz) preserves 768 timed samples
including warm-ups, 16 command logs and corresponding attempt records,
build hashes, the source patch, and the benchmark driver and lockfile.
For reproduction, extract it and place an engine checkout at
`1a827c3` in `engine` alongside `driver`. Use Rust 1.85.0 to build the
driver with `cargo build --release --locked` and save the resulting
binary. Apply `source.diff` in the engine checkout and build a second
binary. Run both in alternating order with disposable fixture storage.
