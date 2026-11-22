# Retained buffer-boundary hashing experiment

**Decision: keep the baseline implementation.** The experiment hashed
base blocks at buffer refill, moved successor hashing underneath the
buffered writer, and reused one scratch buffer for suffix values. It
retained the source recheck and full staged successor verification.

Paired release builds on macOS APFS and Linux ext4 used baseline
`31edb65` and the archived experimental patch. The fixture started
with 131,072 keys and was captured after six epochs with a 60-million
suffix work budget. Each timed mutation used a fresh copy of the same
fixture, wrote 4,096 operations in 32 durable batches, and rotated once.
All 24 result stores reopened and matched the model's 130,700 live
entries. A separate warm-cache scan trial verified the entire state after
epoch six. Binary order alternated in both trials.

## Results

Wall and process CPU times are in milliseconds. Mutations used six pairs
per platform. Verified scans used twelve pairs per platform and exclude
open. Values are medians.

| Platform | Phase | Baseline wall | Experiment wall | Baseline CPU | Experiment CPU |
| --- | --- | ---: | ---: | ---: | ---: |
| macOS APFS | Mutation and rotation | 1,069.02 | 1,078.04 | 144.87 | 124.94 |
| Linux ext4 | Mutation and rotation | 120.76 | 129.22 | 45.44 | 49.03 |
| macOS APFS | Verified scan | 22.55 | 23.51 | 22.16 | 23.36 |
| Linux ext4 | Verified scan | 18.39 | 19.18 | 18.39 | 19.18 |

The macOS mutation CPU median fell, but paired CPU differences changed
sign, and the Linux mutation median was higher. The independent scan
trial showed higher CPU on both platforms. The patch therefore has no
demonstrated cross-platform benefit on this fixture.

The [archive](raw-results.tar.gz) includes the patch, scripts, binary
hashes and phase logs. The first Linux transfer included macOS
AppleDouble metadata and correctly failed closed during reclamation.
The failed log is retained. The paired trials used a new copy transferred
without AppleDouble sidecars. These are local, warm-cache comparisons.
The [SQLite matrix](../2026-11-22-recovery-sqlite-matrix/README.md) describes
the wider retained write and rotation cost.
