# Buffered retained recovery I/O

Revision `e95cc0c` buffers retained base verification, block reads and
staged rebase output. The admitted header, record order, block digests,
complete base digest, suffix binding and synchronization rules still
apply. The source hash manifest matched between macOS ARM64 and Linux
ARM64.

## Same-fixture reopen

Each platform ran eight measured fresh-process pairs on one local
131,072-key fixture after a warmup pair. Both binaries opened the
same generation, suffix and authority revision on every attempt.
The baseline was `a8ccabd`. The two binaries alternated order between
pairs. The table reports the median elapsed time to obtain an open
handle, including authority access.

| Platform | Baseline | Buffered |
| --- | ---: | ---: |
| macOS APFS | 195.48 ms | 71.41 ms |
| Linux ext4 | 150.34 ms | 61.13 ms |

A separate Linux trace on the same fixture counted 392,140 `read`
calls with the baseline and 263 with buffering. Both issued 43
`fsync` calls, six `pread64` calls and four `write` calls. The
[trace summaries](linux-baseline-reopen.syscalls) and
[buffered counts](linux-buffered-reopen.syscalls) cover the named
syscalls only. Timings under tracing are excluded from the table.

## Rotation and recovery workload

The [paired workload](../../../examples/authorized_recovery_bench.md)
also ran end to end with the buffered build on both filesystems. Each
backend applied 32,768 mutations in eight epochs and verified every
live key after every fresh-process reopen. The retained store rotated
42 times under the 1-million work limit and five times under the
60-million limit on each platform. Both backends finished with 130,699
live keys. Buffered retained reopen RSS across these workload phases
peaked at 2.52–4.84 MiB, and sampled write allocation peaked at
43.81–44.30 MiB including the archived log and visible stages.

Raw phase logs and measurements include both work budgets. The large
workload overlapped with testing on the host and VM. Ordinary write
timings varied sharply across campaigns, so those runs do not isolate
the effect on end-to-end rotation latency. RSS covers the process only.
Disk sampling can miss short-lived peaks. Reopen still hashes the
entire admitted base, so the experiment does not establish recovery
latency bounds for arbitrary databases or cold caches. Physical
power-loss and independent off-host restore remain deferred.

## Regression checks

| Platform | Library tests at 16 threads | Selected integration tests |
| --- | ---: | ---: |
| macOS ARM64 | 535 passed | 12 passed |
| Linux ARM64 | 697 passed | 12 passed |

The integration targets were retained indexed reads, retained document
transactions and document process-kill recovery. Formatting and
warnings-denied Clippy passed on macOS. The buffering-specific test
checks the header admission boundary and the exact one-byte EOF
probe, including short reads and rejection of trailing data.
