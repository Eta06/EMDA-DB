# Retained suffix admission before copying the base

On Linux ARM64, the same authorized backup restore test ran against the
baseline in `baseline.txt` and the changed engine. The test first rejects
a valid bound suffix under a zero-operation limit, then restores the
same backup under sufficient limits, registers the result with a fresh
authority and verifies the recovered documents and unique index.

The [baseline trace](before.trace) shows a private base file created in
both restore attempts. The [changed trace](after.trace) shows that the
rejected attempt creates and checks the private log without creating a
base. The admitted attempt creates the base after replay and publishes
the generation. Trace excerpts identify the two distinct staging paths.
Both runs passed the same test and used fresh Rust 1.85.0 dev-profile
builds with debug info and incremental compilation disabled. Temporary
store files were on tmpfs. This checks file creation ordering and
successful reopen. It does not measure elapsed time or disk allocation.

To reproduce, use the changed version of `tests/document_retained.rs`
with both engine revisions. Build it with
`cargo +1.85.0 test --locked --test document_retained --no-run` in fresh
build directories. Run each resulting test binary with:

```sh
strace -qq -f -s 384 -e trace=openat,renameat,renameat2,unlinkat \
  -o import.trace TEST_BINARY \
  --exact authorized_backup_restores_document_indexes_under_a_fresh_authority \
  --test-threads=1
```
