# Authority-backed recovery benchmark

The runner compares ordinary full-log reopen with authority-backed reopen
after an original-path cutover, with an optional SQLite control. All stores start with the same ordered
8-byte keys and deterministic 96-byte values. Each epoch submits 4,096
updates and deletes in 32 durable atomic batches to each backend. It opens
the backend again in a fresh process, then runs a separate process that
compares the complete ordered live state with the expected value and
membership for every key. With SQLite enabled, six epochs cover every
backend order.

```sh
cargo build --release --locked --example authorized_recovery_bench
run=$(mktemp -d "${TMPDIR:-/tmp}/emda-authorized-recovery.XXXXXX")
python3 examples/authorized_recovery_bench.py \
  target/release/examples/authorized_recovery_bench "$run/work-limit" 131072 8
EMDA_BENCH_SUFFIX_WORK=60000000 python3 examples/authorized_recovery_bench.py \
  target/release/examples/authorized_recovery_bench "$run/byte-threshold" 131072 8
EMDA_BENCH_SUFFIX_WORK=60000000 python3 examples/authorized_recovery_bench.py \
  target/release/examples/authorized_recovery_bench "$run/sqlite-control" 131072 6 --sqlite
```

Each run needs a fresh directory and enough space for both fixtures and
the two retained generations that coexist during rotation. Run builds
before timing and keep the tested binary and source stable through each
run. The runner accepts up to 1,048,576 initial keys. Above 262,144
keys it uses a 128 MiB retained-base admission limit and a 256 MiB
source-log limit at cutover. Allow space for the original-path archive
and the optional SQLite database as well. The default suffix budget is
768 KiB, 8,192 operations, and 1,000,000
replay-work units. The policy requests rotation at 640 KiB. The optional
work-budget override must be applied to every phase of the same fixture.
Both the authority directory and the database directories are local. The
authority directory is separate and on the same filesystem for this
benchmark.

The runner saves raw phase logs and JSONL measurements. Write latency is
the time to apply an entire epoch of 32 durable batches, including
synchronous rotation. Reopen duration starts before opening the authority
and database and ends when the database handle is ready. The time to
first verified point read includes open. SQLite uses WAL with
`synchronous=FULL`, a per-value CRC, the default automatic checkpoint,
and last-connection-close checkpointing disabled so a fresh process
can encounter a WAL. A separate phase times a streaming scan against
the workload model on every engine. SQLite's open does less integrity work
than the retained and full-log opens. RSS and total process user and
system CPU time come from the platform's `/usr/bin/time`. CPU readings
from `time` are quantized to hundredths of a second. The runner also
measures phase CPU time with the process clock. It samples allocated bytes,
including visible stages and legacy archives, about every 25 ms. It counts
hard-linked files once per filesystem and inode. Directory traversal can
race publication, and filesystem allocation may be shared or unreported.
The sampled peak describes visible allocation and can miss shorter
peaks. It excludes filesystem metadata and kernel caches.

Fresh processes retain the OS page cache. These are warm-cache runs on
one host and its validation VM. They measure the whole storage paths
with their respective durability work. The authority-backed path also
protects against a stale copy selecting a prior committed endpoint as
long as the authority remains independently protected. This benchmark
does not provide end-to-end memory quotas, cold start at boot, physical
power-loss validation or an independent off-host restore.

See the [cross-platform results](../benchmarks/results/2026-11-19-authorized-recovery-resources/README.md).
The [three-engine matrix](../benchmarks/results/2026-11-22-recovery-sqlite-matrix/README.md)
covers three dataset sizes on macOS APFS and Linux ext4.
The [million-key comparison](../benchmarks/results/2026-11-23-million-key-recovery/README.md)
covers macOS APFS and Linux tmpfs, with a separate rotation I/O trace.
The [authority read comparison](../benchmarks/results/2026-11-22-authority-read-sync/README.md)
isolates record synchronization using the same fixture on each platform.
