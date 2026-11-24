# Retained sparse-index anchor layout on macOS

The [raw archive](raw-results.tar.gz) records both builds, all three
benchmark attempts, complete phase logs and measurements, the paired runner,
binary hashes and the source diff. All 164 file hashes were checked after
extraction. The initial baseline build reused a target directory holding
an older library artifact and failed before timing. Both measured builds
used fresh, separate target directories and Rust 1.85.0 with locked
dependencies. The baseline came from `a5287ca`; the candidate added the
archived sparse-index source patch.

## Three-engine recovery workload

Two sequential macOS ARM64 APFS runs started with 262,144 keys and used
six epochs of 4,096 mutations. All 36 complete ordered verification scans
passed. Both runs rotated the retained generation 31 times. SQLite used
WAL/FULL with per-value CRC. Ready time includes opening the store and
verifying a point read. Times are medians of six fresh-process probes per
engine per build. RSS is the maximum among those probes.

| Build | Engine | Ready wall ms | Ready CPU ms | Max RSS MiB |
| --- | --- | ---: | ---: | ---: |
| Baseline | Ordinary EMDA | 73.32 | 69.76 | 33.69 |
| Baseline | Retained EMDA | 40.72 | 31.68 | 2.59 |
| Baseline | SQLite | 0.94 | 0.67 | 3.20 |
| Candidate | Ordinary EMDA | 72.25 | 69.86 | 33.67 |
| Candidate | Retained EMDA | 36.19 | 28.93 | 2.64 |
| Candidate | SQLite | 0.71 | 0.66 | 3.20 |

## Reopen on the same retained fixture

The paired run alternated ABBA and BAAB on the baseline's final fixture
at authority revision 446 with an 89,532-byte suffix. All 32 fresh-process
reopens checked the expected point value. The preceding full ordered
comparison validated this fixture. Each build contributed 16 samples.

| Build | Median ready wall ms | Wall range ms | Median ready CPU ms | Max RSS MiB |
| --- | ---: | --- | ---: | ---: |
| Baseline | 19.33 | 18.20–21.04 | 15.91 | 2.61 |
| Candidate | 19.59 | 17.47–23.92 | 15.81 | 2.64 |

The candidate consolidates anchor storage and removes one allocation per
block. The paired measurements show overlapping wall times and similar RSS
at this dataset size. The independent fixtures ran in baseline-first order.
Their median difference does not isolate the effect of the index layout.
These warm-cache results use different integrity work across engines. RSS
excludes kernel cache and cannot establish a total memory bound.
