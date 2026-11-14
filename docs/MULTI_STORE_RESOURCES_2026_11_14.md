# Resource use with multiple open stores

This local [measurement](../benchmarks/results/2026-11-14-multi-store-resources.json.gz)
examines the cost of keeping separate databases open in one Linux process. It
does not implement a hosted service or scale-to-zero. On clean revision
`2f6dd97edb1bbbf8d8520b727a7452586ecf31b4`, a release binary opened 1, 10 or
100 independent databases with 1,000 keys each, kept every handle open, and
verified three point reads per database. Keys were 16 bytes and values 64 bytes.
Six fresh processes per size and mode ran in rotated/reversed order. All engine
fixtures used V3; SQLite 3.49.1 used separate WAL/FULL databases and cached
prepared statements.

The table shows medians of six samples. RSS is the whole measurement process
after point reads, not per-store allocation. File-descriptor counts include
four baseline descriptors, including the `/proc/self/fd` enumeration handle.
Open time is the total for all listed databases.

| Open databases | Mode | RSS MiB | Open descriptors | Total open time ms |
| ---: | --- | ---: | ---: | ---: |
| 1 | Ordinary | 2.06 | 8 | 1.78 |
| 1 | Checkpoint-backed | 1.94 | 12 | 0.99 |
| 1 | SQLite | 3.15 | 7 | 0.51 |
| 10 | Ordinary | 3.16 | 44 | 14.27 |
| 10 | Checkpoint-backed | 1.91 | 84 | 4.76 |
| 10 | SQLite | 4.31 | 34 | 3.14 |
| 100 | Ordinary | 14.24 | 404 | 97.85 |
| 100 | Checkpoint-backed | 2.11 | 804 | 44.65 |
| 100 | SQLite | 14.92 | 304 | 28.96 |

This baseline predates the [checkpoint descriptor reduction](CHECKPOINT_VIEW_DESCRIPTORS_2026_11_14.md).
For this fixture, the checkpoint-backed path retained less sampled process
RSS but used about eight descriptors per open database, versus four for the
ordinary path and three for SQLite. Descriptor limits can therefore constrain
the number of simultaneously open databases. Process RSS excludes OS page
cache, filesystem metadata and other processes; small RSS differences can be
measurement noise. These numbers do not establish a total per-project memory
quota or isolation.

The engine's ordinary open replays the log into memory, and its checkpoint
open validates a trusted checkpoint and hashes the covered log. SQLite uses a
different page cache, database format and integrity model. This is not a
feature-equivalent or durability-equivalent ranking. The VM used an AMD EPYC
9V74, a four-vCPU cgroup quota and an overlay filesystem with an unknown backing
device. All initial quiet checks passed, but a short interval reported up to
33.33% VM steal; there was no observed cgroup throttling. Kernel physical-read
accounting was zero in the median of every group, so these were warm-cache
opens, not independent cold-device or crash-recovery measurements. The Rust
compiler was 1.100.0. The raw report includes individual samples and noise
telemetry.

For 100 databases, the median bytes returned by read-like syscalls during open
and sampled reads were about 9.67 MiB for the ordinary path, 19.30 MiB for the
checkpoint-backed path and 2.36 MiB for SQLite. These process counters include
measurement overhead and do not measure physical disk traffic. Checkpoint
opening still hashes the covered log, which matters when evaluating cold starts.

The driver is available for repeatable local measurements:

```sh
cargo build --release --locked --example multi_store_resources
python3 scripts/multi_store_resources.py --directory /path/to/disposable-fixtures \
  --output /path/to/new-result.json --projects 1,10,100 --keys 1000 --rounds 6
```

It creates its own temporary fixtures and requires a new output path. The
measurement does not establish limits for a production deployment, compare a
PostgreSQL server, or measure inactive projects with no running worker.
