# Million-key recovery and authorized local restore

The ordinary full-log store, authority-backed retained store, and SQLite
3.49.1 WAL with `synchronous=FULL` ran the same workload on macOS ARM64 APFS
and Linux ARM64 ext4. The Linux fixture used the virtual block device of
a new disposable guest. The figures include actual ext4 synchronization,
with the backing device on the same physical host as macOS.

Each engine starts with 1,048,576 8-byte keys and 96-byte values.
Each of the six epochs runs 4,096 updates and deletes in 32 durable
batches. Each backend order occurs once. Fresh processes measure open
through the first checked point read, then verify the complete sorted
key/value state against the workload model. SQLite does less integrity
work on open. Past data and caches differ between the engines.

| Filesystem | Engine | Ready wall ms | Ready CPU ms | Reopen peak RSS MiB | Verified scan ms | Write wall ms | Write CPU ms | Peak attributed write MiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| macOS APFS | Ordinary | 193.85 | 170.95 | 127.97 | 350.35 | 184.60 | 3.23 | 128.36 |
| macOS APFS | Retained | 119.77 | 80.06 | 3.05 | 183.80 | 1421.63 | 327.95 | 349.84 |
| macOS APFS | SQLite | 0.80 | 0.57 | 3.12 | 183.39 | 3.86 | 3.24 | 139.68 |
| Linux ext4 | Ordinary | 161.46 | 160.05 | 132.49 | 274.83 | 9.37 | 1.67 | 127.95 |
| Linux ext4 | Retained | 74.75 | 67.02 | 5.13 | 143.88 | 378.74 | 250.94 | 349.85 |
| Linux ext4 | SQLite | 0.49 | 0.49 | 6.12 | 154.46 | 6.64 | 3.10 | 137.70 |

Times are medians over the six epochs. RSS is the maximum process RSS of
the reopen phases. Write allocation is the largest sampled visible
allocation for that backend and can miss shorter peaks. The retained
allocation includes its legacy archive and any coexisting generations.
The retained store rotated four times on each filesystem and its largest
observed suffix was 626,244 bytes. Retained cutover peaked at 254.06 MiB
RSS on APFS and 258.11 MiB on ext4. Cutover and steady-state reopen are
separate operations.

## Backup, retirement and authorized restore

After all epochs, the benchmark takes a bound backup of the current
generation, uses it to authorize retirement of the legacy archive,
and prepares the restore in a separate catalog. The preparation saves the
verified record in a receipt directory before publication. Registration
uses the saved receipt and a fresh authority. It verifies distinct catalog
and generation IDs, matching suffix digest and unchanged source authority.

| Filesystem | Backup ms | Retirement ms | Restore preparation ms | Restore registration ms | Total restore ms | Lifecycle peak RSS MiB | Sampled allocation peak MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| macOS APFS | 119.01 | 202.89 | 214.16 | 108.00 | 322.22 | 3.77 | 349.74 |
| Linux ext4 | 79.09 | 148.12 | 192.56 | 68.08 | 260.66 | 5.29 | 349.77 |

Both platforms verified all 1,048,204 final entries after reopening the
restored catalog and independently checked the source against the same
model. The persisted receipt and restored authority files matched byte
for byte. The ordinary fixture used a V2 witness on macOS and V3 on Linux.
The lifecycle measurement includes current and staged retained data,
backup, restored catalog and authority files. It excludes the ordinary and
SQLite fixtures, kernel cache and filesystem metadata.

## Reproduction and scope

The engine was built from `3b1aad3` with Rust 1.85.0 and the locked
dependencies. The later `0f7ad45` correction changes a rollover test only.
The [raw archive](raw-results.tar.gz) includes the benchmark harness diff,
source and executable hashes, platform records, phase logs and JSONL.
See [the benchmark instructions](../../../examples/authorized_recovery_bench.md)
for the `1048576 6 --sqlite --retire-and-restore --authorized-restore`
command. Run the archive's `summarize.py` with the runner path, then the macOS
and Linux evidence directories to reproduce the summaries.

These are warm-cache runs on one physical Mac and its guest. The runs
overlapped and shared the host storage, so timings can include contention.
The results characterize that workload, including full verification and
synchronous rotation. They do not establish a total process or project
memory cap, physical power-loss behavior, or off-host restore.
