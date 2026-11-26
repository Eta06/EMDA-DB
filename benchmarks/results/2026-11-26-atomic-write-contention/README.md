# Ordinary writers alongside compare-exchange

Before this change, `Store::compare_exchange` and `Store::increment_i64`
held the exclusive write barrier until their record was synchronized.
`write_batch` held it through publication too. Every other writer waited for
that sync, including writers to other keys.

## Change

- **Compare-exchange and increment** still compare under the exclusive
  barrier. Once their record is queued, the barrier is atomically
  downgraded to shared access until the record is synchronized. Records
  queued later are appended after it. Ordinary writers can then queue and
  join the same group commit. Every exclusive writer still waits for the
  acknowledgement: another compare-exchange or increment of any key, a
  transaction, a membership-changing write under live-key limits, a backup
  or a compaction. So a later exchange of the same key reads the
  acknowledged value. Live-key accounting is updated before shared access
  is released.
- **A blind `write_batch`** uses shared access when no live-key limit is
  configured, because it is one record and reads nothing, as an ordinary put
  does. With live-key limits, and for transactions, it keeps exclusive
  access.

Results are visible only after synchronization, as before. A record that
fails to sync fails the store closed, as before.

## Tests

`src/store/atomic_contention_tests.rs` uses a witnessed log whose sync can
be held. While a compare-exchange or increment is syncing:

- an ordinary write to another key reaches the submission queue;
- a second compare-exchange or increment of the same key does not, and
  afterwards it sees the acknowledged value;
- nothing is visible before the sync completes.

A blind batch queues behind a syncing write without key limits, and waits
with them. Each case reopens the store and checks every value. A concurrent
test runs four retrying exchangers and four writers. It checks that all 100
increments and 100 writes are present before and after reopen.

Two mutations were checked. Without the downgrade, both queueing tests
fail. With exclusive batch access, the batch test fails.

## Measurement

`examples/atomic_contention.rs` ran on macOS 27.0, Apple M6, APFS, Rust
1.100.0, release build. Four threads each put 200 distinct keys while two
threads advance one counter with 100 successful compare-exchanges each.
Every write was synchronized. There were seven interleaved paired runs, one
per process.

| Revision | Put p50 | Put p90 | Put p99 | Wall |
| --- | ---: | ---: | ---: | ---: |
| Before (`093128dc`) | 18.1 ms | 23.0 ms | 26.0 ms | 3.78 s |
| After | 12.9 ms | 17.1 ms | 19.1 ms | 2.65 s |

Values are medians of the seven runs. The ranges did not overlap for put
p50 (before 15.9 to 20.1 ms, after 12.1 to 14.0 ms) or wall time (before
3.20 to 4.58 s, after 2.56 to 2.86 s). Every run made exactly 200 successful
exchanges.

The "after" binary was built before the batch change, which this workload
does not exercise. A first set of runs was discarded because both builds
shared a target directory and produced the same binary.
`runs-invalid-same-binary.txt` keeps those runs, and `binaries.sha256`
records the distinct hashes of the runs reported here. This is one host and
filesystem with a synthetic workload. It is not a general ranking.

## Still open

Compare-exchanges and increments of different keys still serialize, and
each holds shared access through its sync. `CheckpointStore` atomics are
unchanged.

[raw archive]: raw-artifacts.tar.gz

The [raw archive] holds both run logs, the workload source, the binary
hashes, the host description and `SHA256SUMS`.
