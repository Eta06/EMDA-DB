# Embedded document workload at up to 5,000 documents

All 44 Linux ARM64 runs passed their operation and reopen checks. Each run preloaded 100, 1,000 or 5,000 small JSON documents with a unique email index, then ran 20 warmup and 200 measured create/read/replace/query/delete cycles. Reopen verification checks the expected live state. EMDA-DB reads every baseline and deleted workload ID by primary key and verifies the complete email index traversal. SQLite checks the complete table and runs `integrity_check`.

The Linux VM used ext4, Rust 1.85.0, EMDA-DB's V3 witness, and bundled SQLite 3.49.1 in WAL mode with `synchronous=FULL`. Four rounds rotated execution order at each size, using one pinned CPU for the benchmark and a separate CPU for the driver. All 44 pre-run quiet windows passed the driver's checks. The benchmark used engine revision `7aa23c2168a1335f4d7ffb3419597012379d766f` with the updated reopen checks in `examples/document_bench.rs` (SHA-256 `88470e8b89b87b6924df07a19053d7c8c89f7f75645cf51ce7f931744335f030`).

Each cell is the median of four per-run query percentiles, in microseconds. The scan is skipped at 5,000 documents because the benchmark's 1,024-document scan budget cannot admit it.

| Preloaded documents | EMDA-DB scan p50 / p99 | EMDA-DB planned p50 / p99 | EMDA-DB explicit index p50 / p99 | SQLite expression index p50 / p99 |
| ---: | ---: | ---: | ---: | ---: |
| 100 | 47.00 / 100.75 | 4.46 / 11.08 | 4.04 / 9.81 | 2.19 / 7.56 |
| 1,000 | 425.98 / 560.07 | 4.60 / 8.31 | 4.19 / 8.10 | 2.02 / 3.15 |
| 5,000 | — | 4.67 / 6.04 | 4.21 / 9.42 | 1.88 / 2.58 |

At 1,000 documents, the planned query took about 1/93 of the collection scan's median time. The explicit index had lower p50 in all three sizes. SQLite's indexed p50 was also lower in all sampled sizes. SQLite returns JSON text in the measured operation and the harness compares its value outside the query timer. EMDA-DB decodes JSON inside its query API. At 5,000 documents the median per-run throughput was 10,896 ops/s for EMDA-DB's planned variant and 26,759 ops/s for SQLite. The workload uses synchronized writes with different storage and acknowledgement mechanisms. It exercises one client with one unique-key query pattern and clean reopening. It leaves cold-cache behavior, concurrency, MongoDB comparison for this dataset, and crash recovery unmeasured.

On macOS, the planned, explicit-index and SQLite variants each passed 100 measured cycles and reopen verification at 5,000 documents. These runs were functional checks.

[Raw Linux results](../benchmarks/results/2026-11-22-linux-arm64-document-scale.json.gz) include individual operation samples, run order, per-process high-water RSS, file sizes, binary and script hashes, and VM telemetry. To reproduce on Linux, choose a writable directory on the test filesystem and run the following from the repository root.

```sh
cargo build --release --locked --example document_bench
python3 scripts/document_scale_compare.py --directory /path/on/test/filesystem \
  --binary "$PWD/target/release/examples/document_bench" --compare-plans \
  --sizes 100,1000,5000 --rounds 4 --cycles 200 --output /tmp/document-scale.json
```
