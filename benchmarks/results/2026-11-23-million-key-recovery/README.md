# Million-key clean recovery and rotation resources

Three six-epoch campaigns compared ordinary full-log recovery, the
authority-backed retained catalog at the original path, and SQLite
WAL with `synchronous=FULL`. The macOS APFS and Linux ARM64 tmpfs runs
each seeded 1,048,576 keys. A second Linux tmpfs run seeded 524,288.
Every engine used the same 8-byte keys, 96-byte values and 4,096
updates/deletes in 32 durable batches per epoch. Each of the six
engine orders ran once. All 54 first reads and 54 complete ordered
live-state comparisons passed. Each retained campaign rotated four
times. Its observed suffix was at most 626,244 bytes.

## One million keys

Ready time includes open and the first verified point. Scan time
covers model verification after opening. The write phase includes
rotation when triggered. Times are medians of six epochs. Peak reopen
RSS and sampled write allocation are maxima across those phases.

| Platform | Engine | Ready wall ms | Ready CPU ms | Reopen RSS MiB | Verified scan ms | Write wall ms | Write CPU ms | Allocated write peak MiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| macOS | Ordinary | 173.75 | 169.38 | 127.88 | 326.44 | 162.26 | 3.76 | 128.44 |
| macOS | Retained | 106.69 | 97.39 | 2.98 | 176.33 | 1282.34 | 346.66 | 349.84 |
| macOS | SQLite | 0.79 | 0.59 | 3.11 | 174.99 | 3.68 | 3.19 | 153.66 |
| Linux | Ordinary | 167.11 | 167.09 | 132.21 | 252.67 | 0.89 | 0.89 | 127.94 |
| Linux | Retained | 69.46 | 69.45 | 3.40 | 138.55 | 245.98 | 245.97 | 349.82 |
| Linux | SQLite | 0.44 | 0.43 | 3.93 | 155.01 | 2.56 | 2.56 | 137.70 |

At 524,288 keys on Linux, ordinary, retained, and SQLite median ready
times were 80.11, 32.08, and 0.56 ms. Their peak reopen RSS was 67.90,
4.83, and 5.89 MiB. Retained cutover peaked at 253.97 MiB RSS on macOS
and 257.93 MiB on Linux at one million keys. Cutover is a separate
operation from retained reopen.

## Rotation file I/O

In the untraced million-key trials on Linux, the four rotation epochs
used 262.29 ms median write CPU, versus 3.03 ms in the two epochs
without rotation. Three extra Linux epochs ran under `strace` and
verified every resulting entry. This diagnostic separates opening
from mutation at the first stdout write, which reports open time.
It counts bytes returned by syscalls on the base file descriptors.

| Epoch | Rotations | Base bytes read on open | Base bytes read during mutation | Base bytes written during mutation | Sync calls during mutation |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 7 | 1 | 117,399,136 | 470,120,272 | 117,398,992 | 419 |
| 8 | 0 | 117,399,136 | 0 | 0 | 341 |
| 9 | 1 | 117,399,136 | 470,119,824 | 117,398,768 | 419 |

Each traced rotation reads roughly two base-sized volumes from the
old generation and one each from the staged and published successor
paths. The trace includes all synchronization calls in the mutation
phase, including the independent authority. These counts identify
where to measure duplicate verification work while preserving the
authority and publication ordering.

## Reproduction and scope

Use the [phase runner](../../../examples/authorized_recovery_bench.md)
with `EMDA_BENCH_SUFFIX_WORK=240000000`, six epochs and `--sqlite`.
The engine revision was `cb8f8ea`. The
[raw archive](raw-results.tar.gz) contains the three sets of phase
logs, environment details, harness diff, and Linux rotation traces.
For the latter, trace `read,pread64,write,pwrite64,fsync,fdatasync`
with `strace -qq -y -s 0` on the retained `mutate` command for epochs
7–9, then run `verify` after each mutation.

Runs use fresh processes with warm caches on one host and its VM.
SQLite checks value CRCs on reads and has a different open-time
integrity scope. Linux tmpfs exercises Linux code and accounting
without timing physical persistence. Allocated bytes are samples of
visible files, include the legacy archive and coexisting generations
when present, and exclude filesystem metadata and cache. Process RSS
excludes the kernel page cache. These checks cover clean recovery and
rotation. Physical power loss and independent off-host restore remain
deferred.
