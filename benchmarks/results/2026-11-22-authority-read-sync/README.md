# File-backed authority synchronization on reads

A file-backed provider validates the record under the authority lock on
every read. At `60c0161`, it retains one file descriptor for its most
recently synchronized record. The provider reuses that synchronization
when the opened record matches the pinned inode and decoded contents.
A replacement or failed sync still requires synchronization before the
provider returns the record. A regression covers two providers observing
an uncertain replacement, same-content replacement on a new inode, and
corruption on the cached inode.

## Matched comparison

Baseline `23a3f32` and candidate `60c0161` ran the same
[recovery example](../../../examples/authorized_recovery_bench.md).
Each platform used one 131,072-key fixture after six mutation epochs with
a 60-million-unit suffix work budget. Each phase used eight fresh
processes per binary, alternating binary order each round. All scans
matched the model's 130,700 entries and all first reads matched the model.

Ready time includes open and the first verified point read. Scan time
excludes open. Wall and CPU are medians in milliseconds. RSS is the maximum
process RSS among the eight corresponding processes, in MiB.

| Platform | Phase | Baseline wall | Candidate wall | Baseline CPU | Candidate CPU | Baseline RSS | Candidate RSS |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| macOS APFS | Ready | 20.147 | 17.796 | 12.797 | 13.003 | 2.781 | 2.828 |
| macOS APFS | Verified scan | 36.414 | 31.093 | 31.493 | 30.014 | 3.125 | 3.188 |
| Linux ext4 | Ready | 10.708 | 10.586 | 10.623 | 10.577 | 4.688 | 4.688 |
| Linux ext4 | Verified scan | 22.631 | 22.280 | 22.630 | 22.279 | 5.094 | 3.152 |

A separate Linux `strace -c -e trace=fsync,fdatasync` run of the complete
verify phase counted **167 baseline and 33 candidate sync calls**. The
traced runs are excluded from the timings. The measurements show less
sync work, a lower median scan wall time on macOS, and a small wall-time
difference on Linux. The peak RSS values are individual high-water marks.

To reproduce, build the release example at each revision and save both
binaries. Seed one fixture with `setup … both 131072 0`, cut over with
`cutover … retained 131072 0`, then run `mutate … retained 131072` for
epochs 1 through 6. Set `EMDA_BENCH_SUFFIX_WORK=60000000` for every phase.
For each binary, measure `reopen … retained 131072 6` and
`verify … retained 131072 6` in fresh processes. Rotate which binary
runs first and use one fixture throughout. See the
[raw logs and binary hashes](raw-results.tar.gz) for both platforms.

These are warm-cache trials on one host and its Linux VM. This isolates
the authority read-path change against the previous implementation. The
[SQLite matrix](../2026-11-22-recovery-sqlite-matrix/README.md) measures
the larger cross-engine trade-offs. This change adds one pinned descriptor
per provider. Rotation, write latency, cold startup, and total memory
limits are outside this comparison.
