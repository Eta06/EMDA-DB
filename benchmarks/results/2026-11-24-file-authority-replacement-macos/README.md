# File-backed authority replacement on macOS

The candidate at `5a5019e` uses a fixed private replacement while
holding the authority lock. Each authority update still synchronizes
the replacement file before rename and the directory after rename.

The macOS ARM64 release benchmark built baseline `26ec336` and the
production candidate in separate target directories with Rust 1.85.0.
Every attempt checked ordinary, checkpoint, authorized retained and
SQLite WAL/FULL against the workload model after reopening.

| Retained mutation workload | Writers | Baseline median, ms | Candidate median, ms |
| --- | ---: | ---: | ---: |
| 4,096 operations, five rotations per epoch | 1 | 1,407.08 | 1,169.30 |
| 4,096 operations, five rotations per epoch | 4 | 1,427.54 | 1,174.31 |
| 32,768 operations, zero rotations | 1 | 7,520.67 | 5,055.96 |
| 32,768 operations, zero rotations | 4 | 7,549.80 | 4,246.91 |

Each row uses two interleaved attempts per binary. The small workload
has two epochs per attempt and the wide-suffix workload has one. Timing
includes acknowledged synchronized writes and any rotation. The two
wide-suffix four-writer candidate samples varied from 3.22 to 5.27 s,
so the median describes this run on one host.

The [raw archive](raw-artifacts.tar.gz) contains every attempt and
comparator phase, the harness, build logs, distinct binary hashes and
the exact production diff. All 59 targeted Rust tests passed.
A separate compatibility run verified a catalog written by the
baseline with the candidate, then verified the candidate's writes
with the baseline. The Linux returned-error campaign runs in
[CI](https://github.com/Eta06/EMDA-DB/actions/runs/45355035630).
