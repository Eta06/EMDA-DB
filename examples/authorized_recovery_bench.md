# Authority-backed recovery benchmark

The runner compares ordinary full-log reopen with authority-backed reopen
after an original-path cutover. Both stores start with the same ordered
8-byte keys and deterministic 96-byte values. Each epoch submits 4,096
updates and deletes in 32 durable atomic batches to each backend. It opens
the backend again in a fresh process, then runs a separate process that
compares the complete ordered live state with the expected value and
membership for every key. Backend order alternates by epoch.

```sh
cargo build --release --locked --example authorized_recovery_bench
run=$(mktemp -d "${TMPDIR:-/tmp}/emda-authorized-recovery.XXXXXX")
python3 examples/authorized_recovery_bench.py \
  target/release/examples/authorized_recovery_bench "$run/work-limit" 131072 8
EMDA_BENCH_SUFFIX_WORK=60000000 python3 examples/authorized_recovery_bench.py \
  target/release/examples/authorized_recovery_bench "$run/byte-threshold" 131072 8
```

Each run needs a fresh directory and enough space for both fixtures and
the two retained generations that coexist during rotation. Run builds
before timing and keep the tested binary and source stable through each
run. The default suffix budget is 768 KiB, 8,192 operations, and 1,000,000
replay-work units. The policy requests rotation at 640 KiB. The optional
work-budget override must be applied to every phase of the same fixture.
Both the authority directory and the database directories are local. The
authority directory is separate and on the same filesystem for this
benchmark.

The runner saves raw phase logs and JSONL measurements. Write latency is
the time to apply an entire epoch of 32 durable batches, including
synchronous rotation. Reopen duration starts before opening the authority
and database and ends when the database handle is ready. The ordinary
duration covers ordinary database open. RSS comes from the platform's
`/usr/bin/time` for each process. The runner samples allocated bytes,
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
