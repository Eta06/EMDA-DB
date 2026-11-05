# Document deadline races, 6 November 2026

[`document_deadline_races.rs`](../tests/document_deadline_races.rs) exercises
deadlines while six writers and a reader operate concurrently. Each writer
tries to update two raw counters, insert a document, update its unique index,
and write a raw marker in one transaction. The check verifies that committed
markers, documents, index entries and counter increments agree, that confirmed
deadline expiries did not submit their writes, and that the same state is
present after closing and reopening the store. It checks both the
ordinary and checkpoint-backed writers.

One writer starts with an expired deadline. Another stages all changes before
releasing the competing writers, then waits until its deadline passes; a guard
releases those writers even if staging fails or panics. Two writers use short
deadlines and two use longer deadlines. Conservative, read-set and FIFO
read-set modes are exercised. A separate reader checks the counter pair while
writes run. IDs are cleaned up after each recovery check so longer runs do not
exhaust the bounded collection and checkpoint overlay.

## Local runs

On a Linux 6.18.57 x86-64 VM using Rust 1.99.1, two runs used 128 histories per
backend: one on `/tmp` tmpfs and one on the workspace overlay filesystem. The
workspace backing device and other host activity were not independently
controlled. Each row represents 768 writer calls and 128 clean reopen checks.

| Filesystem | Writer | Committed | Deadline exceeded | Operation would block | Operation timed out |
| --- | --- | ---: | ---: | ---: | ---: |
| tmpfs | Ordinary | 142 | 258 | 367 | 1 |
| tmpfs | Checkpoint-backed | 254 | 257 | 256 | 1 |
| Overlay | Ordinary | 129 | 256 | 383 | 0 |
| Overlay | Checkpoint-backed | 256 | 256 | 256 | 0 |

All 512 histories passed, including 511 transactions that staged their
changes and then received a confirmed deadline-expired result. In these runs,
the markers for all 1,262 `Operation(WouldBlock)` results and both
`Operation(TimedOut)` results were absent. Generic operation errors are
not treated as confirmed aborts: `WouldBlock` can signal a conflict or
admission failure, and an I/O error can also have an unknown outcome.
`Operation(TimedOut)` can come from a callback, snapshot, or I/O. The check
accepts a complete, consistent committed result or an absent result for either
kind of operation error; it does not assume an error is safe to retry.

The stress runs used the following command, with `TMPDIR` set to the workspace
scratch directory for the overlay run:

```sh
EMDA_DEADLINE_RACE_HISTORIES=128 cargo test --locked --test document_deadline_races -- --nocapture
```

The default 24-history-per-backend test also passed on Rust 1.85.0 and in the
stable optimized build. The run is a finite concurrent test with clean reopens;
it does not inject I/O faults, kill processes, explore every schedule, prove
physical power-loss safety, or establish a performance result. Broader
transaction histories and failure testing remain open.
