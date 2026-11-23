# Recovery allocation probe for standalone log records

Replay borrows standalone record keys from a fixed 1,024-byte scratch buffer.
Ordinary recovery retains owned keys for new index entries. Checkpoint recovery
allocates the immutable overlay key directly from the scratch buffer. Batch
children remain privately staged until all checks on the enclosing DB02
record pass.

## macOS ARM64 results

Each fixture starts with an empty checkpoint and contains 4,096 individual
writes of eight-byte values and 1,024-byte keys. One fixture repeatedly
overwrites 16 keys and the other writes 4,096 distinct keys. The same source
files are opened for both binaries. Each open is measured in a fresh process,
then all live values are checked outside the measured window. Three repeats
per case produced identical results.

| Open path | Distinct keys | Baseline bytes | Scratch-buffer bytes |
| --- | ---: | ---: | ---: |
| Ordinary | 16 | 4,262,472 | 84,552 |
| Ordinary | 4,096 | 4,640,456 | 4,640,456 |
| Checkpoint writer | 16 | 4,287,967 | 93,663 |
| Checkpoint writer | 4,096 | 8,909,171 | 4,714,867 |
| Recovered checkpoint view | 16 | 4,287,180 | 92,876 |
| Recovered checkpoint view | 4,096 | 8,908,356 | 4,714,052 |

For the overwrite-heavy fixture, opens requested about 98% fewer allocator
bytes. Ordinary recovery of distinct keys still needs one owned key per entry.
Checkpoint recovery of distinct keys saves the transient copy before
allocation of the immutable overlay key. This probe measures bytes requested
through the Rust global allocator during each open. It does not measure peak
RSS, elapsed recovery time, batch scratch storage, or a process memory limit.

## Reproduction

Compare baseline `dbdb929` with the commit that adds this report. Copy
[the probe](replay_key_probe.rs.txt) into `examples/replay_key_probe.rs` in
each disposable checkout. Build both using
`cargo +1.85.0 build --locked --release --example replay_key_probe`.
Run the baseline binary with `NEW_FIXTURE_PATH setup 16` and with a second
new path and `setup 4096`. Run each binary against each fixture with
`FIXTURE_PATH MODE KEY_COUNT`, where mode is `ordinary`, `checkpoint` or
`view`. [Raw allocation counts](results.log) include the compiler and host.
