# Heap allocation in conditional value comparisons

The [probe](conditional_alloc_probe.rs.txt) runs 16 comparisons against one
stored value, expecting the key to be absent. Each comparison returns a
mismatch after validating the value. Setup and the final state check run
outside the measurement. Four alternating baseline and candidate pairs
ran per store and value size on Apple Silicon macOS. Both binaries were
built separately with Rust 1.85.0 in release mode. Baseline revision and
candidate diff are in [provenance.txt](provenance.txt).

| Store | Value bytes | Allocation calls before → after | Bytes requested before → after |
|---|---:|---:|---:|
| Ordinary | 8 | 16 → 0 | 128 → 0 |
| Ordinary | 65,537 | 16 → 0 | 1,048,592 → 0 |
| Ordinary | 4,194,304 | 16 → 0 | 67,108,864 → 0 |
| Checkpoint | 8 | 32 → 16 | 544 → 416 |
| Checkpoint | 65,537 | 32 → 16 | 1,049,008 → 416 |
| Checkpoint | 4,194,304 | 32 → 16 | 67,109,280 → 416 |

Allocation totals repeated across all pairs. The counters count requested
heap allocation and reallocation bytes, including checkpoint lookup.
They exclude stack scratch buffers, peak RSS and filesystem memory.
[All 48 samples](samples.log) include elapsed time. The short single-host
workload does not establish contention latency or comparisons with other
engines. Increment and conditional-batch paths were checked functionally
and were outside this allocation measurement.
