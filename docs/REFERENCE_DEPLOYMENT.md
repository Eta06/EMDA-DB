# Reference deployment R1

R1 is a concrete, reproducible configuration for measuring memory, I/O,
rotation and recovery. It stands in for "the intended deployment" in the
foundation checklist until a service exists. **It is a reference
configuration, not a production deployment**, and results on it are not
production measurements.

## Definition

| Aspect | R1 |
| --- | --- |
| Process model | One worker process embeds one project database and is its only writer. |
| Storage mode | `ManagedRetainedStore` catalog: a verified retained base plus a bounded suffix, with two generation slots. |
| Dataset | 1,048,576 live keys at start, 8-byte keys `k0000000` to `k1048575`, 96-byte values. Base about 117 MB. |
| Retained limits | Base 128 MiB and 1,048,576 entries. Suffix 12 MiB, 131,072 operations and 12 billion work units. Overlay 32,768 keys and 256 KiB of key bytes. |
| Catalog quota | Persisted at creation: 512 MiB and 256 entries, covering both slots, one rotation stage and metadata. |
| Workload | One closed-loop thread. Each step makes 4 point reads, then one synchronized atomic batch of 8 distinct keys, about one in 16 a delete. 80% of accesses go to the first 8% of keys. |
| Rotation policy | When `suffix_usage().max_fraction_used` reaches 0.5, `begin_rotation`, then `prepare` on a second thread while the workload continues, then `complete_rotation` and `reclaim_inactive`. |
| Restart | After any exit: `cleanup_abandoned_staging`, `open`, `reclaim_inactive`, then serve. |
| Host | macOS 27.0, Apple M6, 16 GiB, internal SSD with APFS. Release build. Warm page cache. |

The workload is [`examples/reference_deployment.rs`](../examples/reference_deployment.rs).
Its `setup`, `run`, `open` and `verify` phases are separate processes, so
`/usr/bin/time -l` measures each one's peak resident memory. The run and
open phases also report device bytes read and written by the process,
from `proc_pid_rusage` on macOS or `/proc/self/io` on Linux.

## Why these choices

- **Retained catalog.** It is the storage mode whose memory does not grow
  with the number of keys, which a multi-project host needs. The ordinary
  `Store` keeps every key in memory; the
  [million-key comparison](../benchmarks/results/2026-11-24-million-key-wide-suffix/README.md)
  measured about 128 MiB for it at this size.
- **Rotation by occupancy.** A writer would otherwise learn that the
  suffix is full only when a write is refused. `suffix_usage` reports every
  limit that can refuse a write, so the worker can rotate at half
  occupancy and keep half the suffix as room for writes during preparation.
- **One writer thread.** Retained catalogs take `&mut self` for writes, so
  a worker serializes its own writes. Reads in the same thread make read
  and write latency observable on one timeline.

## Measuring it

```sh
cargo build --release --locked --example reference_deployment
bin=target/release/examples/reference_deployment
/usr/bin/time -l "$bin" setup /path/to/r1
/usr/bin/time -l "$bin" run /path/to/r1 0 12000
/usr/bin/time -l "$bin" open /path/to/r1
/usr/bin/time -l "$bin" verify /path/to/r1 12000
```

`run` prints `ACK <batch>` after each batch returns. To test an unclean
exit, kill the process, count the consecutive acknowledgements, then run
`open` and `verify <count>`. Verification streams every entry against the
generated model. Batches below the count must be present. The next batch
may have been in flight, so it must be entirely present or entirely
absent.

## Results

The [2026-11-26 measurements](../benchmarks/results/2026-11-26-reference-deployment-r1/README.md)
cover six steady-state trials and six kill drills at chosen lifecycle
points. The worker peaked at 9.7 to 11.5 MB resident and the catalog at
about 237 MB against its 512 MiB quota. Writers were excluded for about
100 to 150 ms per rotation, mostly while the previous generation was
reclaimed, and three trials had unexplained write stalls of more than a
second. Every kill recovered every acknowledged batch, and the worker
kept rotating afterwards.

## What R1 does not cover

- Other hardware, filesystems, Linux, or cold caches. These runs could not
  purge the page cache.
- Physical power loss. The [power-cut drill](../benchmarks/results/2026-11-26-physical-power-cut/README.md)
  covered an ordinary `Store` on other hardware.
- Several databases per host, network serving, project isolation and
  quotas across databases, which belong to [roadmap step 5](ROADMAP.md#priority-4-secure-service-and-project-isolation).
- Backup and off-host restore of a retained catalog during the workload.
- Memory beyond the worker's own resident set, such as the kernel page
  cache.
