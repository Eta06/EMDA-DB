# Concurrent transaction deadline model — 2026-11-06

A [new reference model](../tests/transaction_deadline_model.rs) checks whether
concurrent deadline outcomes, batch and point writes, reads and the final state
can be explained by a serial order respecting observed operation boundaries.
Each transaction writes two shared keys and its own marker atomically. The
model requires a confirmed `DeadlineExceeded` to leave no marker or other
changes from that transaction. It allows an `Operation` error an unknown
outcome: either no commit or the complete transaction, provided its callback
finished staging. Both writable recovery paths must reproduce the final state.

Each history includes a transaction that attempts to stage before its deadline
and then waits until expiry, a pre-expired transaction, a short-deadline
transaction, a longer-deadline transaction, an atomic batch, a direct write
and two bulk reads. Other operations begin after the first transaction stages
or exits; the longer-deadline transaction begins after the short-deadline
transaction and independent mutations finish, ensuring a useful successful
transaction. Transaction modes rotate across conservative, read-set and FIFO
variants where supported. Histories reset their small fixture between windows.

The initial development run assumed an unconstrained concurrent writer would
always commit, but one sampled configuration produced only conflicts. The test
was revised to coordinate the long-deadline writer as described above; this was
a test scheduling failure, not evidence of an engine defect.

## Recorded run

On clean revision `79b350fa0d1adc9169700669f20c4383577561ae`, stable Rust
1.99.1 in release mode ran 128 histories for each V2/V3, capped/uncapped and
ordinary/checkpoint-writer configuration on a shared Linux VM's workspace
overlay. The [raw result](../benchmarks/results/2026-11-06-transaction-deadline-model.json.gz)
records the command, environment and output.

- 1,024 histories, 8,192 concurrent operations and 2,048 recovery checks passed.
- Of 4,096 transaction attempts, 1,142 committed, 2,575 returned a confirmed
  deadline expiry, 321 returned `WouldBlock` as an operation error and 58
  returned `TimedOut` as an operation error. Among the confirmed expiries,
  1,235 had completed staging first. An operation error is not treated as a
  confirmed abort, and the counts are not a throughput or conflict benchmark.
- Capped ordinary openings limited the log and logical live-key counts and
  bytes; capped checkpoint openings limited the log and suffix overlay. These
  limits do not bound total process memory or all storage.

Reproduce on Linux with:

```sh
EMDA_DEADLINE_MODEL_HISTORIES=128 cargo test --release --locked \
  --test transaction_deadline_model -- --nocapture
```

The default is eight histories per configuration. On other supported platforms
the test covers V2 rather than the Linux-only V3 witness. Rust 1.85 clippy and
the default test also passed locally. The finite model samples schedules; it
does not inject I/O failures, kill the process, simulate physical power loss,
or prove every possible deadline and lock interleaving.
