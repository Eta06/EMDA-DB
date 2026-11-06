# Long mixed transaction histories — 6 November 2026

The [long-history test](../tests/transaction_model/long_history.rs) keeps an
ordinary writable store across successive concurrent windows. Each window
races six optimistic transactions with an atomic batch, a direct write, a
conditional write and two bulk reads. The transactions use conservative,
read-set, FIFO and generously timed modes. The serial model checks observed
results against invocation/return order, the preceding window's final state
and a final bulk read. Every eight windows the store is closed and both
ordinary and checkpoint-backed recovery reopen and verify that state, with
ordinary recovery first. Half the configurations use per-open logical
two-key/two-key-byte caps; these are not total-memory limits.

On clean revision `fe97007b6c0d9a59c39efdfe5e1db8e43d37ad11`, ten release
runs each exercised 256 windows for V2/V3 and capped/uncapped ordinary
writers. All [10,240 windows](../benchmarks/results/2026-11-06-long-transaction-serial.json.gz)
passed, covering 112,640 concurrent operations and 2,560 recovery checks, in
addition to validation reads. Of 61,440 transaction attempts, 4,695 committed
and 56,745 returned conflicts; 229 conditional writes succeeded and 10,011
did not match. This deliberately contended, no-retry test is a correctness
history, not a throughput benchmark or a general prediction of conflict
rates. Each 256-window sequence composes through its verified end states;
the campaign is not one 10,240-window sequence.

```sh
EMDA_LONG_TRANSACTION_WINDOWS=256 cargo test --release --locked --test transaction_model \
  long_history -- --nocapture
```

The workspace overlay's backing device was not independently verified. These
bounded, sampled windows do not exercise a checkpoint-backed writer, expired
deadlines, all interleavings, injected I/O errors, process crashes or physical
power loss. A separate [long transfer test](LONG_TRANSACTION_HISTORY_2026_11_06.md)
covers concurrent transactions on both writable backends.
