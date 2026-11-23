# Checkpoint reads when a suffix exceeds its limit

This paired Linux ARM64 test compares baseline
`04a2e38397beeb1206672e9f68b74d69a0a303a6` with the candidate identified by
[the source hashes](source.sha256). Both used debug builds with debug
information disabled, Rust 1.85.0 and the same tmpfs V3 fixture. The
fixture contains 65,536 base keys, each 64 bytes, with 16-byte values.
The independently retained checkpoint digest covers a 5,374,052-byte
file. The witnessed suffix is 34 bytes.

| Open and verification | Baseline checkpoint bytes / calls | Candidate checkpoint bytes / calls |
| --- | ---: | ---: |
| Suffix cap 33 bytes, rejected | 5,374,052 / 657 | 8,192 / 1 |
| Suffix cap 34 bytes, open and verify two point reads | 5,406,770 / 658 | 5,406,770 / 658 |

Counts sum successful `pread64` return values for `checkpoint.bin`, using
strace 7.0 with `-qq -y -e trace=pread64`. The reject runs each read 36
bytes from `records.log` across two calls. The admitted runs each read
6,625,913 bytes from the log across 107 calls, including the verified
point reads. Each reject returned the suffix-byte limit error and each
admitted reopen verified values from the base and suffix. The checkpoint,
log and anchor file hashes were unchanged. Separate integration coverage
checks preservation of the V2 witness and V3 metadata witness on
rejection. These are application read counts, with no physical I/O or
latency measurement.

To reproduce, add [the probe](checkpoint_admission_probe.rs) to
`examples` and build it with Rust 1.85.0 at the two source states. Use
the first binary to create the fixture with `prepare fixture`. Run both
binaries with `reject fixture` and `accept fixture` under the trace above.
The executable SHA-256 hashes were
`3d961c84ac337ea41ad8d36eeacb3c101eb9c3b3f48cc00ddb684d62a53703a3` and
`922f44f5498debb52ddc9ae22a3de42c9f88ed676519c6dfbe961eff723f25a5`.

All 18 selected integration tests passed on macOS and all 19 passed on
Linux ARM64. Both platforms check rejection before checkpoint entry and
log-prefix validation, corruption rejection when admitted, and recovery
of the expected values at the exact limit.
