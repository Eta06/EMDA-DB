# Atomic guarded index and schema restart under write faults

One read-set transaction restarts a pending unique-index build and a pending
schema migration, then writes a raw receipt. Both jobs begin with a cursor
past the first document. The transaction replaces both cursors and seals in
the collection metadata. Recovery checks the receipt, both cursors, the
maintenance-key requirement, and document and index state through ordinary
and checkpoint opens.

On V2, the fixture writes a clean batch once to locate the metadata child
and final checksum. It then injects a zero-byte append, three torn appends
(first byte, end of the metadata child, and one byte before the end of the
batch), and a failure after synchronizing the complete append. Each partial
append test checks the number of bytes actually written. The zero-byte
append preserves both old frontiers and the absent receipt. A torn batch
fails closed with the log and witness intact. The synchronized batch
recovers both guarded jobs and the receipt together.

On V3, the test reports unknown outcomes for injected errors before and
after the xattr write and before and after inode synchronization. Recovery
adopts the complete batch, preserves the identity anchor, and requires the
key for both jobs. Each successful recovery completes the jobs in a
rolled-back verification transaction and checks that the published schema
would reject a document missing its required field and that the published
indexes match the documents.

Rust 1.85.0 passed ten V2 cases on macOS ARM64 APFS and ten V2 plus eight
V3 cases on Linux ARM64 ext4. The nine guarded-maintenance and retained
document integration tests passed on macOS. The seven Linux metadata
fault tests passed after moving the shared witness injector.

## Reproduction

```sh
cargo +1.85.0 test --locked --lib guarded_maintenance_restart -- --nocapture --test-threads=1
cargo +1.85.0 test --locked --test document_guarded_maintenance --test document_retained
```

Use a temporary filesystem that supports user xattrs for V3 on Linux.
The tests inject returned errors and partial writes while the OS is running.
The [raw results](raw-results.tar.gz) contain the executed test logs and
the source hashes for both runs. The archive SHA-256 is
`8cca776dad52b11bf6bafd2e20149aa1a00382cfe9f2978023e2d7e3f3e8abc4`.
