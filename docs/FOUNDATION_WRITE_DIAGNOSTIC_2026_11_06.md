# Write-path syscall diagnostic, 6 November 2026

The [current engine comparison](FOUNDATION_ENGINE_COMPARISON_2026_11_06.md)
found that SQLite's median write latency was lower in the four-client mixed
and write-only cases. To identify where to investigate next, we traced one
small write-only direct-call run of the prototype and one of SQLite. These
are syscall counts under tracing, not reliable latency measurements.

Both runs used the same release `kv_bench` binary from clean revision
`260c49a`: four clients, 250 measured writes each, 250 initialized keys and
64-byte values on the VM's workspace overlay. Both completed; the prototype
reopened and checked sampled values, while this SQLite harness did not perform
an equivalent reopen check. The prototype used V3 sync-before-acknowledgement
and SQLite 3.49.1 used WAL with `synchronous=FULL`. Initialization and the
prototype's reopen are included in the counts. `strace` 6.13 followed all threads and filtered only the listed
read, write, sync, extended-attribute and futex calls.

| Syscall | Prototype count | SQLite count |
| --- | ---: | ---: |
| `futex` | 3,222 | 306 |
| `fsync` | 710 | 1,264 |
| `fgetxattr` | 1,388 | 0 |
| `fsetxattr` | 694 | 0 |
| `pread64` | 905 | 137 |
| `pwrite64` | 1 | 2,668 |
| `write` + `writev` | 703 | 8 |

The [prototype](../benchmarks/results/2026-11-06-kv-prototype-strace.txt) and
[SQLite](../benchmarks/results/2026-11-06-kv-sqlite-strace.txt) summaries
include the full selected-syscall counts. The benchmark binary SHA-256 was
`0dcabeea1b65949b352eccdcc9f9a2f9ff92e758ae00a8857be64819ca6eb37d`.
The prototype's witness uses xattrs, and independent writes can share a sync;
SQLite uses a different WAL and locking implementation. Tracing changes
scheduling and the number of writes that group together. Futex counts alone
do not measure all waiting: sleep calls were outside the filter. Aggregate
traced syscall times include waits across threads and cannot attribute the
observed p50 difference to a particular cause. Use these observations to
guide targeted profiling and compare changes without weakening the storage
contract, not to claim a performance win.

To reproduce the diagnostic with a separately built release binary:

```sh
strace -f -c -e trace=fsync,fdatasync,fgetxattr,fsetxattr,pread64,pwrite64,write,writev,futex \
  -o /path/to/trace.txt /path/to/kv_bench prototype 4 250 250 100 /path/to/test-filesystem 64
```

Repeat with `sqlite` as the backend. For actual latency comparison, use the
untraced, repeated workload and retain the same durability settings.
