# Sharing the V3 checkpoint writer's log handle

After [releasing validation-only view handles](CHECKPOINT_VIEW_DESCRIPTORS_2026_11_14.md),
the V3 checkpoint writer still held two descriptors referring to the same open
log file description: one for log I/O and one for its metadata witness. It now
shares ownership of one descriptor between those uses. The witness still
checks and updates the metadata boundary and requires a full `fsync` before
acknowledgement. The file remains alive while either owner needs it. V2
checkpoint writers retain their previous handle path; on-disk formats,
transaction semantics and unknown-outcome rules are unchanged.

## Resource and latency measurements

The [paired resource run](../benchmarks/results/2026-11-14-shared-witness-pair.json.gz)
compared clean baseline `8628301` and clean candidate `3cb529e` on the same
100-database V3 fixture, in baseline/candidate/candidate/baseline order. Each
phase had three fresh-process samples and verified 300 point reads. The
fixture's log, witness metadata, identity anchors and checkpoints matched
before and after. Median descriptor totals include four baseline process
descriptors, including the `/proc/self/fd` enumeration handle.

| Mode | Baseline first / repeat | Candidate first / repeat |
| --- | ---: | ---: |
| Checkpoint descriptors | 604 / 604 | 504 / 504 |
| Checkpoint RSS after reads, MiB | 2.09 / 2.11 | 2.11 / 2.04 |
| Checkpoint total open time, ms | 31.37 / 28.90 | 31.24 / 30.72 |
| Ordinary descriptors | 404 / 404 | 404 / 404 |

In this fixture, the second change saved one more descriptor per V3 checkpoint
database. Together with the earlier view change, the sampled total fell from
804 to 504 descriptors for 100 stores. RSS excludes OS page cache. All initial
quiet checks passed; the shared VM reported up to 5.27% steal during the
paired run, with no observed cgroup throttling and no process physical-read
bytes in the samples. These are warm-cache measurements, not a production
capacity limit or general latency claim. SQLite's differently scoped baseline
is in the [multi-store report](MULTI_STORE_RESOURCES_2026_11_14.md).

Three alternating transaction comparisons contain 160 verified runs:
[initial](../benchmarks/results/2026-11-14-shared-witness-transactions.json.gz),
[repeat](../benchmarks/results/2026-11-14-shared-witness-transactions-repeat.json.gz)
and [longer single-client run](../benchmarks/results/2026-11-14-shared-witness-transactions-long.json.gz).
The last used 1,000 transactions per run and six rounds per configuration.
Median p50s for the checkpoint path were 173.46 µs baseline versus 169.82 µs
candidate in its disjoint case, and 170.54 versus 174.87 µs in its shared case.
Results varied in both directions across configurations; they do not establish
a latency improvement or regression. VM steal reached 8.58% in the three
campaigns, and no cgroup throttling was observed.

## Correctness evidence and limits

- Stable debug and optimized all-target suites each passed 936 tests on
  `3cb529e`. Rust 1.85 passed all-target Clippy and focused descriptor-lifetime
  and exhaustion tests on `8969bdd`. The lifetime test checks that the shared
  handle remains usable by the witness and is released after the final owner.
- A [25-run descriptor-exhaustion campaign](../benchmarks/results/2026-11-14-shared-witness-exhaustion.json)
  passed 50 V2/V3 cases and 1,200 parent recovery openings; the
  [log](../benchmarks/results/2026-11-14-shared-witness-exhaustion.log.gz)
  is retained.
- Independently verified reports cover 1,128 selected EIO failures in
  [sync](../benchmarks/results/2026-11-14-shared-witness-eio-sync.json.gz),
  [data](../benchmarks/results/2026-11-14-shared-witness-eio-data.json.gz)
  and [xattr-read](../benchmarks/results/2026-11-14-shared-witness-eio-xattr.json.gz)
  paths, plus [96 concurrent EAGAIN transaction cases](../benchmarks/results/2026-11-14-shared-witness-eagain-transaction.json.gz).
  The [48 interrupted-sync cases](../benchmarks/results/2026-11-14-shared-witness-sync-interrupt.json.gz)
  and [224 short-write cases](../benchmarks/results/2026-11-14-shared-witness-short-write.json.gz)
  include uncertain failures and fail-closed damaged evidence.
- Address-sanitized checkpoint-store and checkpoint-exchange models ran 6,901
  inputs in two 61-second campaigns on `8969bdd`, including starting inputs
  up to 99 and 63 bytes respectively. The [metadata](../benchmarks/results/2026-11-14-shared-witness-fuzz.json)
  and [archive](../benchmarks/results/2026-11-14-shared-witness-fuzz.tar.gz)
  retain the seeds, corpora and logs.

These are finite tests on running operating systems. They do not establish
physical power-loss safety, independent off-host restoration, exhaustive
concurrency, tenant isolation or a total project resource quota.
