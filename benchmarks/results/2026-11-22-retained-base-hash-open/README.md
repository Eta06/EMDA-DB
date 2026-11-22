# Retained base hash update experiment

Combining the encoded lengths and key into one SHA-256 update per hash
did not reduce median startup CPU in this workload. The engine keeps the
existing verification path.

| Implementation | Open wall median (ms) | Open process CPU median (ms) |
| --- | ---: | ---: |
| Retained at `c14a8b6` | 19.724 | 16.137 |
| Retained with combined entry-header hashing | 19.132 | 16.280 |
| Ordinary EMDA at `c14a8b6` | 35.879 | 35.642 |
| SQLite WAL/FULL | 0.821 | 0.609 |

Each sample was a fresh process on macOS 27.0 (26A425), Apple M6 ARM64,
built in release mode with Rust 1.100.0 (0ef1bc386). The fixture contained
262,144 eight-byte keys and 96-byte values. The retained base was
29,360,272 bytes and had an empty suffix. The harness timed open, then
checked a point read. Before sampling, the harness compared all 262,144
key/value pairs against the model for all four variants. There were 28
samples per variant, with a rotating order over a warm fixture. See
[individual timings](samples.json).

SQLite checked the sampled value after open. EMDA authenticated the
complete retained base or ordinary log during open. The samples compare
the cost of the operations each implementation actually performs. The
run covers a single host and key/value distribution and does not resolve
performance under cold-cache or concurrent workloads.

## Reproduce

Build `authorized_recovery_bench` with
`cargo build --release --locked --example authorized_recovery_bench` at
`c14a8b6`, then save that binary as `before`. Apply the
[experiment patch](combined-entry-hash.patch), rebuild, and save the
second binary as `combined`. Make a fresh shared fixture with
`before setup <fixture> all 262144 0` followed by
`before cutover <fixture> retained 262144 0`. For both retained variants
run `verify <fixture> retained 262144 0`, then sample with
`reopen <fixture> retained 262144 0`. Use `before` for the ordinary and
SQLite runs with `ordinary` and `sqlite` in place of `retained`. Rotate
the start position of the order `before, combined, ordinary, sqlite`
once per round and record the reported `open_ns` and `open_cpu_ns`.

The measured baseline binary SHA-256 was
`96b630d34aa7bd801720ceec8adbef1cf95ea679d550e34538f6d75c2932c3f0`.
The combined-entry binary SHA-256 was
`e7df35ff50233be063d3946d8534ce9a222a8c8b84768ad1df8e6e8f431833d0`.
