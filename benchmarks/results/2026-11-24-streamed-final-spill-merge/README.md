# Streamed final spill merge for bounded checkpoints

Linux ARM64 on tmpfs using V3 witnesses, Rust 1.85.0 and the unoptimized
profile. Both full-log rebuilds process 65,536 acknowledged batch children
with 64-byte keys, eight-byte values, 1,024-key replay chunks and fan-in
four. Baseline is `6b3bd95`, and the streamed engine is `2570f87`.

| Workload | Baseline read MB | Streamed read MB | Baseline write MB | Streamed write MB |
| --- | ---: | ---: | ---: | ---: |
| 65,536 distinct keys | 130.81 | 105.37 | 32.38 | 25.63 |
| 4,096 repeatedly updated keys | 171.32 | 165.06 | 15.95 | 15.53 |

The counts are the `rchar` and `wchar` deltas from `/proc/self/io`
during rebuild. They include buffered calls and cached reads. All three
trials per engine and workload reported identical byte counts. Every
trial checked all key/value pairs after reopening the selected checkpoint.
The two engines produced byte-identical checkpoints, and rebuilt source
logs and anchor files matched the fixture. The VM timings in the archive
cover this environment and workload.

The [raw archive](raw-attempts.tar.gz) retains the twelve attempts with
commands, read and write call counts, timings, artifact hashes and build
logs. Its SHA-256 manifest was verified after extraction.

Copy the [probe](final_spill_probe.rs.txt) to
`examples/final_spill_probe.rs` in disposable source copies at the two
revisions and build it with
`cargo +1.85.0 build --locked --example final_spill_probe`.
For each workload create one fixture with `PROBE NEW_FIXTURE setup KEYS`,
where `KEYS` is 65536 or 4096. For each trial copy the fixture's `store`
using `cp -a`, retaining its witness, and run
`PROBE TRIAL_DIR VARIANT KEYS`. Check the checkpoint SHA-256 and compare
the source log and anchor bytes with the fixture.
