# Checkpoint view descriptor reduction

A [later V3 writer-handle change](CHECKPOINT_SHARED_WITNESS_2026_11_14.md)
reduced the same 100-store sample by another 100 descriptors.

The multi-store baseline found that the checkpoint-backed writer held eight
descriptors per open V3 database. A read-only checkpoint view kept its own
commit-witness and identity-anchor handles after validating the checkpoint,
even though later view reads use the retained log and checkpoint files plus the
captured boundary. The writable store already owns separately validated write
handles and continues to check its witness and synchronize commits.

The change releases the view's validation-only witness and anchor handles
after checkpoint verification. The read-only log retains its data descriptor;
the view retains the checkpoint and cooperative store lock. An internal guard
rejects append, synchronization or witness verification on a versioned log
without its witness, rather than allowing it to be mistaken for a writable
log. Legacy log behavior and on-disk formats are unchanged. Views still verify
checkpoint blocks and value checksums, and the writable path retains its
existing durability and unknown-outcome rules.

## Resource comparison

The [paired run](../benchmarks/results/2026-11-14-checkpoint-fd-pair.json.gz)
used the same 100-database V3 fixture for clean baseline `2f6dd97` and clean
candidate `8628301`, ordered baseline, candidate, candidate, baseline. Every
sample reopened all stores and checked 300 point reads; fixture hashes and
metadata matched before and after. Each phase contains three fresh-process
samples. The table shows phase medians; descriptor totals include four baseline
process descriptors, including the `/proc/self/fd` enumeration handle.

| Mode | Baseline first / repeat | Candidate first / repeat |
| --- | ---: | ---: |
| Checkpoint descriptors | 804 / 804 | 604 / 604 |
| Checkpoint RSS after reads, MiB | 2.10 / 2.11 | 2.10 / 2.11 |
| Checkpoint total open time, ms | 29.24 / 28.89 | 29.23 / 28.48 |
| Ordinary descriptors | 404 / 404 | 404 / 404 |

The checkpoint-backed path saved two descriptors per V3 database in this
fixture. RSS excludes OS page cache. Open times are warm-cache samples, not a
general speed claim: all initial quiet checks passed, but the shared VM
reported up to 4.17% steal during the runs. No cgroup CPU throttling was
observed and process physical-read accounting was zero in every sample.

A separate [64-run transaction comparison](../benchmarks/results/2026-11-14-checkpoint-fd-transactions.json.gz)
and [16-run repeat](../benchmarks/results/2026-11-14-checkpoint-fd-transactions-repeat.json.gz)
verified the workload after reopening. Timing was mixed. For example, the
first four-client disjoint checkpoint read-set sample had median p50 of
281 µs baseline versus 512 µs candidate; the longer repeat measured 410 µs
versus 348 µs. These small, noisy samples do not establish a latency improvement
or regression. The largest recorded VM steal was 6.25%; no cgroup throttling
was observed. The SQLite resource baseline is reported separately in the
[multi-store comparison](MULTI_STORE_RESOURCES_2026_11_14.md).

## Correctness evidence and limits

- The local stable debug and optimized all-target suites each passed 933 tests
  on candidate `8628301`. Rust 1.85 passed all-target Clippy and the real
  descriptor-exhaustion test. The exhaustion test covers V2 and V3, writes
  through an already-open store at the limit, retries a failed open after
  freeing descriptors, and verifies both writable recovery paths. A
  [25-run stress campaign](../benchmarks/results/2026-11-14-checkpoint-fd-exhaustion-stress.json)
  passed 50 V2/V3 cases and 1,200 parent recovery openings; its
  [test log](../benchmarks/results/2026-11-14-checkpoint-fd-exhaustion-stress.log.gz)
  is retained.
- Three independently verified EIO reports cover 1,128 selected sync,
  data-write and xattr-read failures across both formats and recovery orders:
  [sync](../benchmarks/results/2026-11-14-checkpoint-fd-eio-sync.json.gz),
  [data](../benchmarks/results/2026-11-14-checkpoint-fd-eio-data.json.gz),
  and [xattr](../benchmarks/results/2026-11-14-checkpoint-fd-eio-xattr.json.gz).
  A further [96 concurrent transaction EAGAIN cases](../benchmarks/results/2026-11-14-checkpoint-fd-eagain-transaction.json.gz)
  checked 157 acknowledgements and atomic recovery of failed requests.
- An address-sanitized checkpoint-store model ran 9,401 inputs in 61 seconds
  on clean revision `1ef6f30`, starting with 13 seeds; its generated input
  length limit was 27 bytes at completion. The [metadata](../benchmarks/results/2026-11-14-checkpoint-fd-fuzz.json) and
  [archive](../benchmarks/results/2026-11-14-checkpoint-fd-fuzz.tar.gz)
  contain the log, original seeds and resulting corpus.
- The checkpoint exchange model ran 9,538 inputs in two 61-second campaigns,
  including four new 63-byte starting sequences that exercise longer
  rollover, compaction and reopen combinations. Its
  [metadata](../benchmarks/results/2026-11-14-checkpoint-fd-exchange-fuzz.json)
  and [archive](../benchmarks/results/2026-11-14-checkpoint-fd-exchange-fuzz.tar.gz)
  retain the seeds, corpora and logs. These models are finite and serial.

These finite tests do not cover every schedule or filesystem failure. They do
not establish physical power-loss safety, independent off-host restoration,
total project resource limits, tenant isolation, or scale-to-zero behavior.
