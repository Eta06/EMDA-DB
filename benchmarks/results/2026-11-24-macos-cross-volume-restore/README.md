# macOS cross-volume restore drill

The [raw archive](raw-artifacts.tar.gz) records a release build and restore on
macOS ARM64 using Rust 1.85.0 at `38e8988`. The source and restored store were
on the host volume. The backup was on a freshly mounted disposable APFS
disk image with a distinct filesystem device ID. The image was detached
when the run finished. All seven archived file hashes verified after
extraction.

With the source moved out of its original path, the drill reopened the
restored store and verified all 99 expected values, the deletion, and the
absence of an acknowledged write made after the backup. The archive records
the toolchain, binary hash and raw output. The accompanying macOS backup
concurrency test also passed. Two Linux-only test binaries selected by the
same command each ran zero tests on macOS.

The disk image shares one host and its underlying storage. This exercises
local backup and restore across filesystem boundaries with the OS running.
Physical power-loss testing and the independent off-host restore remain
deferred.
