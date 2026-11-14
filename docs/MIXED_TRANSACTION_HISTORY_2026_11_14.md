# Mixed transaction and direct-operation histories

The two [local runs](../benchmarks/results/2026-11-14-mixed-transaction-history.json)
used clean revision `2f2b1aa8164c5b3385cde516405a246680239751` on a Linux VM.
Each run checked 4,096 concurrent histories, once using tmpfs and once using
the workspace overlay filesystem. The workspace's underlying storage is not
independently characterized. Both runs passed, and both writable recovery paths
reopened and checked the final state after each history.

Every history begins with two present keys and one absent key, then starts eight
operations together: a range-reading transaction, a point-reading transaction,
a put, a delete, a compare-exchange, an atomic batch, a scan and a bulk read.
The transactions stage changes based on the values they observe. The model
searches for a serial explanation that respects non-overlapping operations'
real-time order and the final state. Transaction modes and ordinary, deadline
and cooperative-cancellation APIs rotate across the runs. On Linux, V2/V3 and
ordinary/checkpoint-backed writers each account for one quarter of the
histories. Both compare-exchange outcomes and both committed and confirmed
conflicting transactions occurred. Negative controls check that the oracle
rejects stale range/absent-point reads, fractured batches and incorrect
compare-exchange outcomes.

| Fixture filesystem | Histories | Committed transactions | Confirmed conflicts | Compare success / mismatch |
| --- | ---: | ---: | ---: | ---: |
| tmpfs | 4,096 | 3,319 | 4,873 | 2,748 / 1,348 |
| Workspace overlay | 4,096 | 2,834 | 5,358 | 2,800 / 1,296 |

Each row contains 8,192 attempted transactions. The model is finite and the
OS chooses the sampled interleavings. These are clean reopens, not process
kills, physical power loss or an independent off-host restore. They do not
prove every schedule, cover distributed or multi-process writes, establish
tenant isolation, or compare performance. See the
[storage contract](STORAGE_TRANSACTION_CONTRACT.md) and
[remaining foundation work](FOUNDATION_FOLLOW_UP.md) for scope and open gaps.
