# Concurrent updates and deletes across process kills — 6 November 2026

The [test](../tests/concurrent_kill_history/updates.rs) runs four writers in one
process, each repeatedly replacing and deleting its own key. Every operation
emits a start event before submission and an acknowledgement event only after
returning successfully. The parent kills the child after observing a selected
number of acknowledgements, then drains the remaining complete pipe events.
Each writer's recovered value must match its last acknowledged operation or
its one started but unacknowledged operation. A delete is checked as an absent
key. A started operation may not yet have reached the engine, so its result is
uncertain. The two writable recovery paths must produce the same values; an
invalid partial log may instead fail closed without altering the log or
witness. Previously acknowledged guard data and the original log prefix are
also checked. Negative controls reject a lost acknowledged value, a lost
acknowledged deletion and an impossible later value.

On clean revision `8d91f36fb0bfededee09d46b5aac3386b7ed9e0f`, the
[50-run release campaign](../benchmarks/results/2026-11-06-concurrent-update-kill.json.gz)
completed 800 process-kill cases: V2 and V3, ordinary and checkpoint-backed
writers, and four acknowledgement thresholds in each run. It observed 36,397
acknowledged operations, including 11,070 deletes, plus 3,189 started operations
without a complete acknowledgement event. Both paths reopened successfully in
all cases and returned values allowed by the per-writer histories. An earlier
value may have been superseded, so this checks the latest acknowledged or
in-flight state rather than requiring every historical value to remain visible.
The writer's backend reopened first, followed by the other backend.

The fixture used the workspace overlay, whose underlying device was not
independently verified. The target is observed through a pipe; scheduling is
sampled, and a write is not guaranteed to be in its syscall when killed. This
is not a cross-process multi-writer test, an exhaustive serializability proof,
a physical power-loss test or an off-host restore drill.
