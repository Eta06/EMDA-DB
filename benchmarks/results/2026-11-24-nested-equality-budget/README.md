# Nested JSON equality work limits

Revision `913f8b6` adds two cumulative limits to the existing document
filter evaluation budget. Each attempt admits up to 1,000,000 nested
child comparisons and 64 MiB of logical equality text bytes. A compared
array child or a matching object member uses one nested comparison.
Object key lookups charge the key length. Equal-length strings charge
their length before comparison. An exact bound is accepted. An overrun
returns `InvalidInput` instead of a partial answer. A planner retry in
document-ID order starts a fresh filter budget.

The array fixture matches 125,000 children eight times. The query
succeeds at one million comparisons. Comparing one extra child rejects
both scanned and indexed queries and their aggregation paths. The text
fixture repeats eight 256 KiB comparisons across 32 candidate rows,
fits exactly at 64 MiB, and rejects a transaction's staged thirty-third
document. The receipt and the staged document both roll back on ordinary
and checkpoint-backed stores. A unit test confirms object key charging
and compares the bounded equality result with serde_json on nested values.

The same integration tests compiled against `891a0e6` reproduce both
gaps. The over-budget scan returns a matching document and the over-budget
indexed query allows its transaction to commit. Both older budget tests
still pass. The raw baseline test output is included alongside the
passing focused results.

Native macOS Rust 1.85.0 passed all 1,328 tests across 112 nonempty
binaries. Warnings-denied Clippy and rustdoc passed. Hosted [Linux]
passed both toolchains and the optimized stable suite. The hosted
[macOS] stable and Rust 1.85.0 jobs both passed. The [filesystem fault] and [ENOSPC]
workflows passed. Physical power-loss testing and independent off-host
restore remain deferred.

The [raw archive](raw-artifacts.tar.gz) includes all 45 standard smoke
attempts, five [cross-engine smoke] attempts, and the
bounded four-writer recovery workload. The workload passed all 51
phases, including 24 at a 48 MiB address-space limit. All attempt results,
raw output, baseline regression logs and file checksums are included.

[Linux]: https://github.com/Eta06/EMDA-DB/actions/runs/45380616015
[macOS]: https://github.com/Eta06/EMDA-DB/actions/runs/45380616017
[filesystem fault]: https://github.com/Eta06/EMDA-DB/actions/runs/45380616116
[ENOSPC]: https://github.com/Eta06/EMDA-DB/actions/runs/45380615876
[cross-engine smoke]: https://github.com/Eta06/EMDA-DB/actions/runs/45380615960
