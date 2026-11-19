# Retained rotation source reads

Engine revision `d4f92aa4dfefdadde7488ef39e8a90bf3e90266b` computes
successor size during the authenticated source scan. It checks the base
length, canonical header, trailer and every block, then checks the whole
witnessed suffix while reconstructing historical peaks. The merge emits
the successor through fixed buffers. Rotation verifies the synced output
and rechecks the complete source before selection.

The tests cover header and trailer substitution, corrupted data hidden
by a tombstone, a rechecksummed superseded suffix value, an empty output
at exactly 144 bytes, and one-byte-short file and work limits. A failed
frame check leaves both the selected generation and its witness intact.

## Linux ARM64 read trace

The [rotation workload](../2026-11-19-streamed-retained-rebase/rotate.rs)
creates a 28 MiB base and a 4 MiB suffix. Each release binary opened and
rotated a separate copy of the same catalog. `strace` recorded read calls
on the old generation. The table includes the initial open and rotation.

| Build | Bytes read from source base | Bytes read from source log |
| --- | ---: | ---: |
| Previous, `2b995e0` | 176,161,578 | 25,167,594 |
| Combined sizing and history scan, `d4f92aa` | 117,441,196 | 16,778,658 |

Both successors occupied 25,166,042 bytes and reopened with the same seven
keys and all expected values. A normalized SHA-256 comparison matched the
successor bytes after accounting for the fresh base ID and dependent digest.
The source files in both copies matched the original fixture hashes. The
read counts measure logical bytes returned by the kernel on this workload.
The measurement includes the synchronous rotation path and uses a warm
fixture on the VM's tmpfs. [Trace totals](summary.json).

To reproduce, build `rotate.rs` as a release example at both revisions in
isolated target directories. Run `rotate setup` with the previous binary,
copy the resulting managed catalog for each run, and trace each binary's
`rotate` phase with `strace -y -s 0 -e trace=read,pread64,readv,preadv,preadv2`.
Sum successful reads of the descriptors labeled `/generation-a/base>` and
`/generation-a/records.log>`, then use the corresponding binary's `verify`
phase on the rotated catalog.

## Validation

Both ARM64 hosts used identical source and test inputs. Library suites ran
at 16 threads. The selected integration targets covered atomic batches,
indexed retained reads, future format guards and retained document
transactions. Formatting and warnings-denied Clippy passed on macOS.
[Source manifest](source.sha256), [toolchains](environment.log).

| Platform | Library tests | Selected integration tests |
| --- | ---: | ---: |
| macOS ARM64 | 558 passed | 20 passed |
| Linux ARM64 | 720 passed | 20 passed |

Physical power loss and independent off-host restore remain deferred.
