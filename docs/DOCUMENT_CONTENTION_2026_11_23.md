# Concurrent document updates and indexed reads

All 36 Linux ARM64 runs completed and verified their data after clean
reopening. Six macOS smoke runs also passed. The Linux workload preloaded
500 JSON documents with a unique email index. Each of 1, 2, or 4 clients
ran 100 cycles. A cycle acknowledged a replacement that changed one
client's email and immediately looked up the new email through the
index. Clients updated distinct documents and contended on the same
database and index. Every queried ID and decoded JSON value matched the
acknowledged replacement.

Both EMDA-DB backends used read-set transactions and synchronized V3
writes. SQLite 3.49.1 used one connection per client, an equivalent
unique expression index, WAL mode, and `synchronous=FULL`. The SQLite
adapter decoded JSON and checked the result inside its query timer.
EMDA-DB's query timer includes construction and execution of the planned
query and verification of the selected access path. The SQLite update
timer includes JSON encoding. Both update timers include any bounded
retry. The driver recorded zero confirmed EMDA-DB conflicts and eight
returned SQLite busy retries, all at four clients. Time spent inside
SQLite's 50 ms busy handler remains in the update latencies.

## Linux ARM64 results

Throughput covers both operations in each cycle. Each table cell gives
the median across four runs. Latencies give the median of per-run
nearest-rank p50 and p99, in microseconds.

| Clients | Engine | Operations/s | Update p50 / p99 µs | Indexed read p50 / p99 µs | Peak RSS KiB |
| ---: | --- | ---: | ---: | ---: | ---: |
| 1 | EMDA-DB Store | 7,622 | 255.92 / 415.08 | 5.67 / 20.35 | 5,832 |
| 1 | EMDA-DB CheckpointStore | 4,682 | 318.58 / 650.19 | 87.10 / 142.92 | 5,980 |
| 1 | SQLite | 5,799 | 320.69 / 530.69 | 2.71 / 10.31 | 6,602 |
| 2 | EMDA-DB Store | 6,469 | 330.46 / 850.36 | 50.62 / 906.38 | 7,882 |
| 2 | EMDA-DB CheckpointStore | 4,331 | 597.86 / 1,299.94 | 184.73 / 754.71 | 9,702 |
| 2 | SQLite | 5,095 | 317.52 / 742.02 | 2.81 / 14.15 | 8,736 |
| 4 | EMDA-DB Store | 8,602 | 398.63 / 2,113.80 | 305.31 / 1,926.65 | 12,894 |
| 4 | EMDA-DB CheckpointStore | 5,395 | 801.46 / 2,666.99 | 473.73 / 1,512.69 | 12,172 |
| 4 | SQLite | 7,426 | 208.50 / 591.40 | 2.71 / 20.23 | 13,008 |

At four clients, EMDA-DB Store sustained 7,837–8,999 operations/s
across runs. SQLite sustained 6,182–7,440 operations/s and recorded an
indexed-read p99 of 20.23 µs, compared with 1,926.65 µs for Store and
1,512.69 µs for CheckpointStore. The measurements identify read latency
under writes as a place to profile. The benchmark does not attribute
the delay to a particular lock or establish a general ranking.

The driver used Rust 1.85.0 release binaries built against engine
`42d3a98`, ext4 fixtures, and four available VM CPUs. Engine order
rotated each round. VM-wide I/O wait reached 19.05% in a run.
Preparation and clean reopen verification are outside operation timings.
Peak RSS is the Linux process high-water mark across all phases and
excludes kernel page cache. It does not establish a memory quota. In
addition to their different transaction and durability mechanisms, the
engines differ in storage representation and index maintenance. This
small closed-loop workload leaves open-loop demand, same-document
contention, MongoDB, cold-cache reads, and crash recovery unmeasured.

## Verification and reproduction

On reopen, EMDA-DB compares every primary document and traverses the
complete email index. SQLite verifies the row count, every primary and
indexed result, and `integrity_check`. The macOS smoke runs used one
and four clients, 256 documents, and 20 cycles per client on each engine.

[Raw Linux samples](../benchmarks/results/2026-11-23-linux-arm64-document-contention.json.gz)
include all latencies, retry counts, per-run throughput, telemetry, and
binary and source hashes.

```sh
cargo +1.85.0 build --release --locked --example document_contention_bench
python3 scripts/document_contention_compare.py \
  --binary target/release/examples/document_contention_bench \
  --directory /path/on/test/filesystem --output /tmp/document-contention.json.gz \
  --source-revision "$(git rev-parse HEAD)" --clients 1,2,4 \
  --rounds 4 --cycles 100 --documents 500
```

## Read-only validation follow-up

On Store, read-only transactions can validate against the published
index during log synchronization. They still detect changed observations,
cancellation, deadline expiry, and uncertain writes. On CheckpointStore,
read-only validation uses the shared state lock, while writes hold the
exclusive lock through the commit. Both backends retain their exclusive
write-commit path.

The [paired raw results](../benchmarks/results/2026-11-23-linux-arm64-read-validation.json.gz)
compare `cff3a8a` with `0e94bb8` on the same 500-document workload.
All 75 runs verified every primary document and the index after reopening.
The Linux ARM64 VM ran on ext4 with four available CPUs and Rust 1.85.0
release builds. Five rounds rotated both engine revisions and the SQLite
reference. This run was separate from the macOS fault-suite workload.
VM-wide I/O wait reached 16.67%.

| Clients | Store read p99 before / after, µs | CheckpointStore read p99 before / after, µs | SQLite read p99, µs |
| ---: | ---: | ---: | ---: |
| 1 | 15.62 / 14.75 | 116.29 / 117.71 | 11.12 |
| 2 | 565.58 / 13.58 | 443.79 / 397.50 | 11.79 |
| 4 | 1,442.88 / 18.17 | 1,350.92 / 1,041.38 | 14.21 |

The four-client Store read p99 fell from 1,442.88 to 18.17 µs. The
before runs ranged from 1,134.46 to 1,776.88 µs, and the after runs
ranged from 11.83 to 29.42 µs. Median throughput rose from 9,007 to
9,431 operations/s, while update p99 rose from 1,465.75 to 1,975.25 µs.
One single-client Store after run had a read p99 of 491.71 µs. The
checkpoint-backed store still had substantially higher concurrent
read tails than SQLite. These observations apply to this closed-loop
workload and filesystem.

To reproduce the paired run, build the same example at the baseline
revision, preserve its binary, and add
`--baseline-binary /path/to/before/document_contention_bench`
and `--baseline-revision cff3a8a` to the command above. Use five rounds
and the candidate's source revision.

## Checkpoint reads during synchronization

CheckpointStore now holds a writer gate through append and sync, while
releasing the state lock so readers can use the published view. A
synchronized batch replaces the overlay atomically. Full-log copies
still wait on the writer gate, and append failures and panics leave
the instance failed until reopen.

The [paired raw results](../benchmarks/results/2026-11-23-linux-arm64-checkpoint-sync-reads.json.gz)
compare `beacc6d` with `a86b213` using separate Rust 1.85.0 release builds
on Linux ARM64, 500 documents on ext4, and five rotated rounds. All 75
runs verified the primary documents and email index after reopening.
VM-wide I/O wait reached 11.54%. Use separate Cargo target directories
when building each revision for reproduction. The driver refuses
byte-identical baseline and candidate binaries.

| Clients | CheckpointStore read p99 before / after, µs | CheckpointStore update p99 before / after, µs | SQLite read p99, µs |
| ---: | ---: | ---: | ---: |
| 1 | 115.04 / 108.33 | 362.75 / 316.04 | 9.29 |
| 2 | 410.46 / 234.96 | 606.00 / 633.67 | 15.62 |
| 4 | 1,060.29 / 252.50 | 2,495.88 / 1,427.67 | 14.71 |

At four clients, the checkpoint read p99 range dropped from
1,047.92–1,091.92 µs to 248.38–262.67 µs. Median throughput increased
from 6,470 to 6,811 operations/s. Median update latency increased from
739.08 to 960.21 µs, even as the update tail fell. The two-client update
p99 also increased in this sample. Checkpoint-backed reads still had
higher p99 than SQLite and the ordinary store at each tested client count.
The measurements cover this workload and leave cold-cache behavior,
resource ceilings, and crash recovery outside the comparison.
