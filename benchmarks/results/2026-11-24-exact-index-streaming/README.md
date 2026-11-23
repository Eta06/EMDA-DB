# Exact-index document query allocations and latency

A single complete equality probe visits verified index entries in document ID
order. The planner now passes those entries directly to the query collector,
avoiding a second owned copy of each ID. Other access paths still keep IDs
when needed for deduplication and ordered error reporting.

## Allocation probe

The [probe](exact_alloc_probe.rs.txt) queries 1,024 matching documents with
256-byte IDs on an ordinary store, with an offset of 900 and a limit of 16.
It counts allocator requests during the query and checks the complete page
against a collection scan outside the measured window. The three fresh
processes in each case produced the same counts.

| Variant | Allocation requests | Requested bytes |
| --- | ---: | ---: |
| Baseline | 24,856 | 2,556,937 |
| Streamed IDs | 23,661 | 2,244,513 |

The change saved 312,424 requested bytes (12.2%). These are allocated byte
requests during the query. Peak RSS and total transaction memory are separate.

## SQLite comparison

The existing compound equality benchmark uses a separate workload of 1,024
documents with 256 matches returned in full. Each binary ran 100 timed cycles
per variant after 10 warmup cycles. Four paired rounds alternated the order
of the baseline and changed binaries. The harness checks every result and
verifies the reopened data in both engines. SQLite 3.49.1 used WAL and FULL
synchronization, with its expression index verified in the query plan.

| Query | Baseline p50 / p99 (µs) | Streamed IDs p50 / p99 (µs) |
| --- | ---: | ---: |
| EMDA indexed | 220.02 / 383.12 | 202.97 / 334.58 |
| EMDA collection scan | 547.57 / 919.92 | 542.31 / 923.79 |
| SQLite indexed | 77.97 / 134.60 | 76.92 / 138.33 |

Each cell reports the median of the four per-round percentiles. The indexed
EMDA median improved by 7.7%. SQLite remained faster in this single-client,
in-process read workload. The samples do not cover concurrent writes or
independently cold caches.

## Reproduction

Compare baseline `429c77d` with the commit that adds this report. In a
disposable checkout, copy the probe to `examples/exact_alloc_probe.rs` and run
`cargo +1.85.0 run --release --locked --example exact_alloc_probe` three times.
For timing, build `compound_query_bench` with the same compiler and release
profile, then invoke each binary with `SCRATCH_DIR 1024 256 100`. The host and
compiler are in the [allocation log](allocations.log). The
[raw timing samples](compound-query.jsonl.gz) include variant, round and the
verified SQLite query plans.
