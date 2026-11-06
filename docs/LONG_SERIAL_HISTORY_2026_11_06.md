# Long concurrent key-value histories — 6 November 2026

The [history test](../tests/delete_reinsert_model/long_history.rs) keeps a
store open across successive concurrent windows. Each window races nine
independent puts, deletes, value comparisons, an atomic batch and reads on two
keys. A serial model checks the observed results against invocation/return
ordering and the previous window's final state. A final bulk read constrains
the window's resulting state. Every eight windows, the store is closed and
both ordinary and checkpoint-backed recovery paths reopen it and verify that
state. Ordinary recovery opens first in these checks.

On clean revision `760bd89eac4ebcffc55b7d930105827aa5c6c8bb`, ten release
runs used 256 windows for each V2/V3 and ordinary/checkpoint-writer
configuration. All [10,240 windows](../benchmarks/results/2026-11-06-long-serial-history.json.gz)
passed, covering 92,160 concurrent operations and 2,560 recovery checks,
in addition to the final validation reads. The histories included 10,472
unsuccessful and 10,008 successful conditional comparisons. A completed
window's observed state becomes the next window's starting state, so each
256-window sequence composes into a valid history; these are not 10,240
windows in one uninterrupted sequence.

The following runs one such four-configuration sequence on a supported Linux
filesystem:

```sh
EMDA_LONG_SERIAL_WINDOWS=256 cargo test --release --locked --test delete_reinsert_model \
  long_history -- --nocapture
```

The campaign used the workspace overlay, whose backing device was not
independently verified. It samples thread schedules in bounded windows, with
clean reopens; it does not exhaust all interleavings, test multi-process
writers, inject I/O failures, or simulate a physical power loss.
