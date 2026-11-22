# Retained overlay batch replay on reopen

At `be92a16`, a recovered batch contributes its final edits after
record verification and ordered admission. Dense inserts use the same
in-place merge as durable appends. Strictly ordered keys past the last
overlay key extend the overlay directly. Both paths can move keys
out of the verified decoder. The physical bytes, checksum and witness
checks, and ordered operation, work, and overlay limits still apply.

## Paired reopen measurements

Baseline `0e7b1c9` and candidate `be92a16` seeded the same number of
entries above an empty retained base. Each trial wrote another durable
batch of 1,024 operations. The timer covers reopen after that batch.
The full key and value verification after reopening is outside the
timer. All 96 final trials matched the model.

Times are six-pair medians in milliseconds. Each pair alternated
binary order. Builds used separate Cargo target directories.

| Platform | Seeded keys | Final batch | Baseline wall | Baseline CPU | Candidate wall | Candidate CPU |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| macOS APFS | 8,192 | Insert before | 17.323 | 13.042 | 11.302 | 1.718 |
| macOS APFS | 65,536 | Insert before | 124.555 | 118.150 | 12.499 | 8.241 |
| macOS APFS | 65,536 | Insert after | 14.048 | 11.003 | 10.533 | 7.054 |
| macOS APFS | 65,536 | Replace | 30.315 | 12.415 | 22.507 | 8.314 |
| Linux ext4 | 8,192 | Insert before | 12.462 | 12.098 | 1.350 | 1.004 |
| Linux ext4 | 65,536 | Insert before | 97.166 | 96.036 | 6.865 | 6.547 |
| Linux ext4 | 65,536 | Insert after | 8.841 | 8.318 | 6.321 | 5.978 |
| Linux ext4 | 65,536 | Replace | 7.869 | 7.540 | 6.054 | 5.793 |

These are local warm-cache reopens. The empty base isolates suffix replay.
The [SQLite matrix](../2026-11-22-recovery-sqlite-matrix/README.md)
covers full-base startup and end-to-end retained workloads. The initial
macOS general-merge trial, also archived, used more CPU for the append
and replacement controls. The final implementation adds the ordered
append path used while seeding those workloads.

The [archive](raw-results.tar.gz) contains the probe, driver, raw runs
and environment details. Copy the probe into
`examples/retained_overlay_merge_probe.rs` at each revision, build the
release example into separate target directories, and name the binaries
`baseline` and `candidate` in one run directory. Run the archived
`run-paired.sh` with that directory and a separate writable fixture
parent. The tests and benchmark evidence do not establish a total
memory bound or a recovery time bound for full bases.
