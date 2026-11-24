# Bounded checkpoint rebuild with an in-memory final chunk

The two-record trusted-suffix regression admits a 100-byte spill cap.
The previous streamed merge needed 192 bytes to hold its two one-key
runs and the final chunk's offset sidecar. Rejection at 99 bytes
preserves the pending witness. The full-replay fixture still uses
324 bytes for its earlier merge. A separate regression verifies the
spill fallback when an older large run occupies the fan-in.

The [probe](final_chunk_probe.rs.txt) compares `718b308` and `5ba79f2`
on a Linux ARM64 VM with tmpfs, V3 witnesses, Rust 1.85.0 and an
unoptimized build. Both full-log rebuilds consume 6,144 acknowledged
batch children with 64-byte keys and eight-byte values. Replay uses
1,024-key chunks and a fan-in of four.

| Workload | Baseline read bytes | Updated read bytes | Baseline write bytes | Updated write bytes |
| --- | ---: | ---: | ---: | ---: |
| 6,144 distinct keys | 6,699,364 | 6,301,736 | 1,559,257 | 1,453,724 |
| 4,096 keys with repeated updates | 8,597,143 | 8,199,515 | 1,391,321 | 1,285,788 |

The counts are the `rchar` and `wchar` deltas from `/proc/self/io`
during rebuild. All three trials per engine and workload produced the
same byte counts. Every trial reopened the result, checked all live
key/value pairs, compared checkpoint hashes across versions and
compared the source log and anchor with the fixture. These are logical
I/O counts from this workload on the running VM.

The [verified archive](raw-attempts.tar.gz) includes all 12 benchmark
attempts, command traces, timing and I/O counters, and build logs.
The updated build log records 13 bounded rebuild integration tests
and all 18 pending-witness syscall cases.

Copy the probe into `examples/final_chunk_probe.rs` in disposable
source copies at both revisions. Build with
`cargo +1.85.0 build --locked --example final_chunk_probe`. Create a
fixture with `PROBE NEW_FIXTURE setup KEYS`, using 6144 or 4096 keys.
Copy its `store` with `cp -a` into a new trial directory and run
`PROBE TRIAL_DIR VARIANT KEYS`.
