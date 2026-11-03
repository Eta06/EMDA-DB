# Recovery fuzz target

The `recovery` target feeds malformed legacy and witnessed logs to the public
open path. It asserts that malformed input rejected during validation leaves its
log and witness unchanged. I/O failures during witness publication can have an
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
