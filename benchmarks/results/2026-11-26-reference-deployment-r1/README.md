# Reference deployment R1: memory, I/O, rotation and recovery

Measurements on the [R1 reference deployment](../../../docs/REFERENCE_DEPLOYMENT.md):
one worker process owning a managed retained catalog of 1,048,576 keys, with
occupancy-driven rotation jobs and a 512 MiB catalog quota. Revision
`74a4343c` plus the `suffix_usage` accessor and the workload in this
commit. **These are reference results, not production measurements.**

Host: macOS 27.0, Apple M6, 16 GiB, internal SSD with APFS, Rust 1.100.0,
release build, warm page cache.

## Steady state

Six trials, each a fresh APFS clone of one fixture, ran 12,000 steps: 48,000
point reads and 12,000 synchronized eight-key batches. Each trial rotated five
times. After every trial a fresh process reopened the catalog, and another
streamed all 1,044,290 live entries against the model. All matched.

| Trial | Steps/s | Write p50 | Write p99 | Write p99.9 | Write max | Read p50 | Read p99 | Worker peak RSS |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 0 | 94 | 5.7 ms | 58.6 ms | 185 ms | 4,831 ms | 0.11 ms | 0.30 ms | 11.4 MB |
| 1 | 145 | 5.9 ms | 9.7 ms | 21.7 ms | 30 ms | 0.25 ms | 0.31 ms | 10.2 MB |
| 2 | 95 | 6.9 ms | 28.3 ms | 49.5 ms | 2,065 ms | 0.26 ms | 0.32 ms | 10.7 MB |
| 3 | 142 | 6.0 ms | 8.8 ms | 12.1 ms | 28 ms | 0.24 ms | 0.31 ms | 11.5 MB |
| 4 | 184 | 4.9 ms | 7.5 ms | 18.9 ms | 41 ms | 0.11 ms | 0.15 ms | 10.1 MB |
| 5 | 105 | 6.9 ms | 37.0 ms | 62.3 ms | 1,269 ms | 0.24 ms | 0.31 ms | 9.7 MB |

Trials 0, 2 and 5 each had write stalls of more than one second. Trial 0
overlapped a test-suite build on the same host. Nothing else was running
during trial 5, and the stalls are unattributed: per-write timestamps were
not recorded, so they cannot be matched against rotation phases or host
activity.

### Memory and disk

- **Worker memory.** Peak resident memory was 9.7 to 11.5 MB with about
  117 MB of data. It includes a rotation preparing the successor.
- **Catalog footprint.** It peaked at 237.3 to 237.4 MB while two
  generations coexisted, against the 512 MiB quota, and returned to about
  118 MB after each reclaim.
- **Device writes.** Each run wrote 765 to 771 MB. Each rotation wrote 114 to
  121 MB, mostly the successor base. The remaining roughly 165 MB comes to
  about 14 KB per synchronized batch.
- **Device reads.** At most 1.6 MB per run, because the cache was warm.

### Rotation

There were 30 rotations, and each began when one suffix limit was half used.

| Phase | Typical | Range | Writers |
| --- | ---: | ---: | --- |
| `begin_rotation` | 4 ms | 1.6 to 17 ms | Excluded |
| `prepare` | 1.5 to 2.0 s | 0.75 to 5.7 s | Continue: 158 to 257 batches per job |
| `complete_rotation` | 22 to 37 ms | 10 to 352 ms | Excluded |
| `reclaim_inactive` | 67 to 115 ms | 54 to 850 ms | Excluded |

The suffix was 4 to 6% used after each rotation. Write p99 during
preparation was 9.9 to 50 ms. Writers were therefore excluded for about 100
to 150 ms per rotation. The largest share was `reclaim_inactive`, which
removes the previous generation under the catalog's mutable borrow. The
worst case was about 1.2 s, in trial 0.

### Restart

A fresh process cleaned stages, opened the catalog, reclaimed the inactive
slot and read one key in 103 to 132 ms. Peak resident memory was 3.8 to
4.0 MB, and it read no device bytes. The full verification scan took 130
to 140 ms.

## Kill drills

The driver sent SIGKILL to the worker six times, at chosen lifecycle
points. Each drill started from a fresh clone, then restarted the worker
and verified the catalog. It ran 2,600 further steps and verified again.

| Drill | Operator action | Last event before the kill | Acknowledged batches | Stages removed | Old slot reclaimed at restart | In-flight batch present | Restart | Restart RSS |
| --- | --- | --- | ---: | ---: | --- | --- | ---: | ---: |
| 0 | Random time, 15.8 s | First prepare | 2,355 | 1 | No | Yes | 167 ms | 6.0 MB |
| 1 | Random time, 3.9 s | None | 548 | 0 | No | Yes | 102 ms | 3.4 MB |
| 2 | 0.84 s into prepare | Second prepare | 4,490 | 1 | No | Yes | 157 ms | 6.2 MB |
| 3 | 0.19 s into prepare | Second prepare | 4,408 | 1 | No | Yes | 115 ms | 6.0 MB |
| 4 | On the complete event | Second complete | 4,596 | 1 | No | No | 128 ms | 6.1 MB |
| 5 | On the reclaim event | Second reclaim | 4,607 | 0 | Yes, 87 ms | No | 152 ms | 3.1 MB |

The machine observations for every drill:

- Every acknowledged batch was present.
- The batch in flight at the kill was either entirely present or entirely
  absent.
- Every other key matched the model.
- An interrupted preparation left one stage, which cleanup removed.
- In drill 4, the kill arrived before the successor was published. The
  previous generation stayed selected.
- In drill 5, the successor had been published. The previous generation
  remained in the inactive slot and was reclaimed at restart.
- After each restart, the worker completed one or two further rotations,
  and verification passed again.

## Limits

- One host, filesystem and warm cache. The page cache could not be purged
  without elevated privileges.
- Process kills, not power loss.
- One synthetic workload and one database per process. RSS covers the
  worker only, not the kernel cache.
- The write stalls above one second are unexplained.
- Kills aimed at an event land shortly after the worker reports it, not at
  an exact instruction.

[raw archive]: raw-artifacts.tar.gz

The [raw archive] holds every phase's JSON, including `/usr/bin/time -l`
resources, along with both drivers, the workload source, the redacted
environment record, including the binary hash, and `SHA256SUMS`.
