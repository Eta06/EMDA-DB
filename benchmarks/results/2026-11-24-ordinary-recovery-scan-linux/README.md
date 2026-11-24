# Ordinary recovery scan admission on Linux

The ordinary store now accepts an optional inclusive logical log scan
limit at recovery. With a pending witnessed tail, the limit includes the
full-log confirmation before witness adoption. Both Linux V2 and V3
fixtures had a 139-byte log. With a 277-byte budget, each attempt read
only the 8-byte magic and 28-byte identity header, rejected before
replay, and preserved the source log and witness. With 278 bytes, each
attempt performed two 139-byte scans and adopted the tail. The exact
syscall trace also includes the identity reads and the probe's final
17-byte point read.

Both source revisions passed the returned-error campaign, including
38 witness and selection injections per run with recovery checks.
Both also passed the separate disposable ENOSPC check. The
[raw archive](raw-artifacts.tar.gz) preserves those reports, job status,
and all six cross-engine attempts across the two revisions. The
cross-engine workloads cover compound range, document contention and
large crash/reopen. They are smoke runs with checked outputs.

| Source | Filesystem faults | ENOSPC | Cross-engine attempts |
| --- | --- | --- | --- |
| `b76f6b0` | [EIO](https://github.com/Eta06/EMDA-DB/actions/runs/45359818589) | [ENOSPC](https://github.com/Eta06/EMDA-DB/actions/runs/45359818490) | [Benchmarks](https://github.com/Eta06/EMDA-DB/actions/runs/45359818524) |
| `42a9fec` | [EIO](https://github.com/Eta06/EMDA-DB/actions/runs/45360180672) | [ENOSPC](https://github.com/Eta06/EMDA-DB/actions/runs/45360180643) | [Benchmarks](https://github.com/Eta06/EMDA-DB/actions/runs/45360180647) |
