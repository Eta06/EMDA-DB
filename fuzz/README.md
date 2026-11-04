# Recovery fuzz target

The `recovery` target feeds malformed legacy, V2 externally witnessed, and V3 metadata-witnessed logs to the public
open path. It asserts that malformed input rejected during validation leaves its
log and witnesses unchanged. V3 seeds include the required identity anchor and xattr bytes. The first input byte uses bit 7 for an external witness, bit 6 for metadata, and the lower six bits for external-witness length; optional metadata follows with its own one-byte length. I/O failures during witness publication can have an
uncertain outcome and are treated separately.
The checked-in seeds are synthetic. Install nightly Rust and `cargo-fuzz`, then
copy the seeds to a temporary corpus before running so generated inputs do not
modify the repository:

```sh
mkdir -p /tmp/database-engine-fuzz-corpus
cp fuzz/seeds/* /tmp/database-engine-fuzz-corpus/
cargo +nightly fuzz run recovery /tmp/database-engine-fuzz-corpus -- -max_total_time=60 -max_len=8192
```

The campaign exercises parser and recovery behavior; it does not simulate
storage persistence, prove all input cases safe, or replace power-loss testing.
The [recorded initial campaign](../benchmarks/results/2026-11-03-recovery-fuzz.json)
ran 127,313 inputs in 61 seconds without a detected crash. A scheduled and
manually triggerable GitHub Actions workflow repeats a short campaign.

The `key_byte_limit` target compares single writes, deletions and ordered
batches against a small reference model, checks rejected writes do not change
the log, and repeatedly reopens with the same logical live-key byte cap. Its
checked-in synthetic seeds are in `fuzz/key-byte-limit-seeds`. This tests the
per-open key-byte accounting, not total memory, concurrent scheduling,
physical power loss, or persistent project quotas.

The `log_limit` target models encoded record lengths and key/value state under
a per-open log-length cap, including atomic batches, rejected writes and
reopening. Its seeds are in `fuzz/log-limit-seeds`. It is single-threaded and
does not replace concurrent reservation, I/O fault or power-loss tests.
