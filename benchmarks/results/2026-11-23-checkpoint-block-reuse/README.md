# Checkpoint block reads within one operation

Point, bounded range, and bulk reads now share one verified checkpoint block
within the operation. Every block loaded into the buffer still receives a
full SHA-256 check. The next operation starts with an empty buffer. On a
multi-block checkpoint, lookups can still alternate between blocks during
binary search, which limits how much one buffer can save.

## Paired Linux ARM64 measurements

The baseline (`ba29db7`) and candidate (`5b63c66`) used independent Rust 1.85
release builds and the same witnessed fixture on tmpfs. Each fixture had no
suffix and used 100–128-byte keys sharing a long prefix. Both builds
verified every returned key and value. These are positional reads from
the checkpoint descriptor, counted after opening the store (and after
capturing the snapshot for snapshot reads). Log value reads are excluded.

| Fixture | Operation | Reads before / after | Bytes returned before / after |
| --- | --- | ---: | ---: |
| 160 keys, one block | Point | 2 / 1 | 42,030 / 21,015 |
| 160 keys, one block | Range, 8 results | 2 / 1 | 42,030 / 21,015 |
| 160 keys, one block | Bulk, 96 keys | 192 / 1 | 4,034,880 / 21,015 |
| 160 keys, one block | Snapshot bulk, 96 keys | 192 / 1 | 4,034,880 / 21,015 |
| 640 keys, multiple blocks | Point | 3 / 2 | 98,199 / 65,433 |
| 640 keys, multiple blocks | Range, 8 results | 3 / 2 | 98,199 / 65,433 |
| 640 keys, multiple blocks | Bulk, 96 keys | 288 / 192 | 9,427,104 / 6,281,568 |
| 640 keys, multiple blocks | Snapshot bulk, 96 keys | 288 / 192 | 9,427,104 / 6,281,568 |

The probe measures bytes returned by the kernel in these fixed queries.
It does not measure elapsed time, physical device I/O, or total memory.

The focused suites passed 71 tests on macOS and 69 integration tests on
Linux ARM64. The new bulk regression covers ordering, duplicate and absent
keys, overlay changes, and full-block corruption detection on a later
store or snapshot read.

## Reproduction

The [raw evidence](linux-evidence.tar.gz) includes the probe, trace parser,
individual traces, and toolchain and binary hashes. Put
`checkpoint_read_probe.rs` under `examples` in both revisions and build
each copy with a different `CARGO_TARGET_DIR`. Run setup once per fixture,
using either binary. Run each operation with both binaries against
the same fixture. The parser counts reads between the probe's markers.

```sh
CARGO_INCREMENTAL=0 cargo +1.85.0 build --locked --release --example checkpoint_read_probe
"$binary" setup "$fixture" 160
for operation in point range bulk snapshot; do
  strace -qq -y -s 64 -e trace=pread64,read,write -o "$operation.trace" \
    "$binary" "$operation" "$fixture"
  awk -v label="$operation" -f count.awk "$operation.trace"
done
```
