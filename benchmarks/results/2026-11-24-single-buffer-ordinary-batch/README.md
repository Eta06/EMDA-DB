# Ordinary DB02 batch allocation and admission

The [probe](batch_alloc_probe.rs.txt) submits one 4 MiB value as a blind
batch or a transactional write. The rejection cases use a whole-log cap
with no remaining bytes or disable group admission before the call. Every
accepted write was checked after reopen. Each rejection preserved the log
length and left the key absent. Setup and verification are outside the
measurement. Four baseline and candidate pairs ran per case on macOS
ARM64, using separately built Rust 1.85.0 release binaries.

| Case | Heap allocation requests before → after | Requested bytes before → after |
|---|---:|---:|
| Blind batch | 16 → 15 | 8,394,661 → 4,200,328 |
| Transaction | 25 → 24 | 12,589,889 → 8,395,556 |
| Rejected by log limit | 5 → 4 | 4,194,488 → 155 |
| Rejected by group admission | 5 → 3 | 4,194,476 → 103 |

Each total repeated across the four pairs. The counters count requested
heap bytes, including reallocations and the transaction's staged value.
The figures do not measure RSS or filesystem allocation. Elapsed times
are in the [32 raw samples](samples.log). These are single-host
microbenchmarks with equivalent durability on both revisions.

[Source provenance](provenance.txt) records the candidate diff and the
baseline revision. The unit test compares the appended record against
the previous two-buffer encoder. It also rejects submission at exact log
and group capacity, verifies both reservations release, and retries the
same sequence.
