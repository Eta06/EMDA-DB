# Checkpoint admission from block capacity

The one-block fixture packs 32,768 index bytes into a 32,868-byte file.
At that limit, the view opens and returns the expected value. The test
then changes the magic byte and appends one byte. For historical,
recovered read-only, and writable checkpoint opens, the extra byte is
enough to prove that the payload cannot fit.

On Linux ARM64 with ext4 and Rust 1.85.0, the [baseline trace](baseline-index.strace)
at `30410ad` records one 8,192-byte index read on each impossible open.
The [current trace](current-index.strace) records the size checks and
zero index bytes read on all three opens. When the extra byte is removed,
verification reads the checkpoint and detects the changed magic.
Restoring the original bytes allows a clean reopen. Neither the source log
nor its witness file changes during these checks.

The packing regression verifies that an index can still require more
blocks than its total byte length suggests. The streaming writer test
checks zero block capacity and a one-byte-short file cap before its entry
callback, then writes and reopens the checkpoint at the exact file limit.

The traces retain index descriptor operations and their results, with
fixture paths and process IDs removed. They measure returned index bytes
and exclude source-log reads, filesystem cache behavior and wall time.
The focused checkpoint limit and bounded rebuild suites passed on macOS
and Linux ARM64. The writer callback test passed on macOS.

## Reproduction

Build the included test with
`cargo +1.85.0 test --locked --test checkpoint_view_limits --no-run`.
Use the executable path printed by Cargo for `TEST_BINARY` and run on Linux
with `TMPDIR` on a disposable ext4 workspace.

```sh
strace -f -qq -yy -v -e trace=statx,newfstatat,fstat,pread64 -o trace.log \
  TEST_BINARY oversized_block_payload_is_rejected_before_parsing_on_all_opens --exact
```

For the baseline, copy only the updated test into `30410ad` and repeat.
The baseline fails the new assertion because it parses the damaged
header on all three opens.
