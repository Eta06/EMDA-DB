# File-backed retained authority validation

Tested engine revision `78e5ab0`. The library and integration runs on macOS
and Linux ARM64 used matching Rust sources, test inputs and Cargo files. See
the [SHA-256 manifest](source.sha256) and
[platform and compiler record](environment.txt).

| Check | macOS | Linux ARM64 |
| --- | ---: | ---: |
| Library suite, 16 threads | 523 | 685 |
| Targeted document, indexed candidate and format guard tests | 11 | 11 |

Two independent processes compared against the same authority revision.
Exactly one replacement succeeded, and a fresh read matched that process's
proposal. The interruption test killed a child before rename and again after
rename but before directory sync. Reopening synchronized and returned one
complete visible record in each case. Additional tests reject skipped
revisions, missing or damaged records, and a catalog rollback after rotation
while the authority remains current.

## Local cross-filesystem probe

The [probe archive](linux-provider-probe.tar.gz) includes its build inputs,
mount output and program output. The Linux ARM64 VM hosted the catalog on
tmpfs and the bundled file authority on ext4. Separate processes prepared and
wrote the catalog, rotated and reclaimed a slot, read the acknowledged values,
and attempted to open an older copied catalog. The authority retained revision
six and rejected the older copy.

To repeat, set the probe's path dependency to a checkout of `78e5ab0`
and build offline in release mode. Run `file-authority-probe seed CATALOG AUTHORITY`
with the catalog and authority on their respective mounts. Copy the catalog
directory to a separate stale path. Run `rotate CATALOG AUTHORITY`,
`verify CATALOG AUTHORITY`, then `rollback STALE_CATALOG AUTHORITY`.
The authority directory provides protection when it is independently
protected from catalog rollback. This local VM exercise leaves the OS
running. Physical power-loss and independent off-host restore remain
deferred.

Raw logs are available for [macOS library](macos-lib.log.gz),
[macOS integration](macos-integration.log.gz), [Clippy](macos-clippy.log.gz),
[Linux library](linux-lib.log.gz) and [Linux integration](linux-integration.log.gz).
Linux used Rust 1.85.0, cached offline dependencies and two build jobs.
macOS passed formatting and warnings-denied all-target Clippy.

The four focused file-authority tests also passed with `TMPDIR` on the
[VM ext4 volume](linux-ext4-filesystem.txt). This repeats the contention
and process-kill checks on the file-backed mount from the probe.
See the [ext4 test log](linux-ext4.log.gz).
