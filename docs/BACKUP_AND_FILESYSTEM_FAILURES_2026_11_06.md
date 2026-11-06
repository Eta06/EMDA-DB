# Backup and filesystem failure checks — 2026-11-06

These local checks ran on clean revision
`373b00ae73161ec21dedca5a4b990eb939fc74ce`. They extend the existing
[synchronization EIO campaign](SYNC_EIO_EXTENDED_2026_11_06.md) and do not
establish production readiness, physical power-loss durability, independent
off-host recovery or a performance ranking. The machines and procedures for
the physical and off-host drills remain deferred to later sessions.

## Returned filesystem errors

The Linux driver traces each successful fixture operation, then injects one
`EIO` at each selected fixture call. Each injection is repeated with the
ordinary and checkpoint-backed recovery paths reopening first. The verifier
checks acknowledged values, source log prefixes, the reference backup and
checkpoint, any visible copy, staging ownership and subsequent cleanup. It
checks existing partial initialization evidence before and after reopening.
Initialization uses the filesystem's default new-store format; compacted
checkpoint digests computed from the output are not independent authentication.

| Call group | Injected cases | Results |
| --- | ---: | --- |
| Data writes and metadata | 190 | All returned an error. |
| Reads | 820 | All returned an error. |
| Opens | 618 | All returned an error. |
| Directory and namespace operations | 144 | 88 returned an error; 56 returned success after an unlink error. |

All 1,772 cases completed their checks. The 56 successful results are cleanup
unlink failures after publication; each visible copy was checked and cleanup
was retried after injection stopped. Forty-eight namespace cases had staging
names visible before that cleanup. Failed writes can have unknown outcomes.
An absent initialization target may stay absent, and an empty target can be
initialized during verification; existing rejected evidence must remain intact.

The raw results include operation, format, injected syscall and ordinal,
recovery order, outcomes, file digests and scope:

- [Data and metadata](../benchmarks/results/2026-11-06-filesystem-eio-data.json.gz)
- [Reads](../benchmarks/results/2026-11-06-filesystem-eio-read.json.gz)
- [Opens](../benchmarks/results/2026-11-06-filesystem-eio-open.json.gz)
- [Namespace operations](../benchmarks/results/2026-11-06-filesystem-eio-namespace.json.gz)

`strace` injects errors into a process on a running OS. It does not simulate
partial success of the injected syscall, all later error-only calls, arbitrary
concurrent failures, device-cache behavior or physical persistence ordering.
V2 and V3 source fixtures do not imply that new-store initialization was
tested in both formats. The [failure matrix](FOUNDATION_FAILURE_MATRIX.md)
explains the driver and the remaining limitations.

### A second failure during cleanup

On clean revision `b0677500a84e5eba6a234d3bb059a3181a1eb3bd`, the
[two-error campaign](../benchmarks/results/2026-11-06-double-eio.json.gz)
injected a data or namespace error at each of 167 selected calls, identified
the synchronization calls subsequently reached, and repeated each with a
second `EIO` and both recovery orders. All 258 resulting double-error cases
completed their checks; every second injected error was `fsync` in this run.
There were visible staging names before cleanup in 194 cases, none afterward,
and 48 visible checkpoint-compaction copies were checked. The copied
checkpoint's digest was recomputed from that copy and is not independent
authentication. This samples pairs of returned errors, not all combinations
or the physical effects of a power failure.

## Backups racing atomic writers

The [concurrent backup test](../tests/backup_concurrent_model.rs) sends four
independent writers one update per round while a backup is requested. Writers
alternate atomic batches and transactions; documented optimistic conflicts are
retried within a bound. Each update writes the same sequence number to two
keys. After restoring each backup, the test checks that each pair is equal,
that every update acknowledged before the request is included, and that no
writer's value is beyond its observed progress plus its one possible
unpublished acknowledgement. The source is checked again after the final
round using checkpoint recovery.

Twenty optimized repetitions completed 960 backup/restore rounds: 240 for each
V2/V3 and ordinary/checkpoint-writer combination. In 842 rounds at least one
writer's acknowledgement counter had not reached the current round when the
backup returned; in 643 rounds at least one had reached it. These categories
can overlap, and an outstanding counter does not prove the writer was executing
at that instant. The raw [stress record](../benchmarks/results/2026-11-06-concurrent-backup-stress.json)
contains every run and the observed counters. The first development version of
the test encountered an expected optimistic conflict; the recorded campaign
uses bounded retries for that specific conflict and fails on other errors.

## Backup reference-model fuzzing

The address-sanitized `backup_roundtrip` target generated 5,048 inputs over
181 reported seconds without a finding. It compared puts, deletes, ordered
batches and transactions with an independent map before and after checkpoint,
backup, a later source write and restore. Both writable backends and V2/V3
fixtures are represented in its seeds. The fuzzer's generated input-length
limit reached 22 bytes in this run, so it exercised up to five generated
four-byte operations per input; longer seeds were added afterward.

The [machine-readable record](../benchmarks/results/2026-11-06-backup-roundtrip-fuzz.json),
[full log](../benchmarks/results/2026-11-06-backup-roundtrip-fuzz.log.gz) and
[final synthetic corpus](../benchmarks/results/2026-11-06-backup-roundtrip-corpus.tar.gz)
are retained. The model is serial and does not inject errors, establish
concurrent backup behavior, simulate a crash or perform an off-host restore.
To replay it, extract the synthetic corpus into a new temporary directory and
run `cargo +nightly-2026-11-03 fuzz run backup_roundtrip <corpus-directory> -- -runs=0`.

A second address-sanitized run on clean revision `b067750` started from eight
seeds, including four 98-byte cases with 24 operations each. It completed
5,182 generated inputs in 181 reported seconds without a finding, and its
generated input-length limit reached 98 bytes. The [record](../benchmarks/results/2026-11-06-backup-roundtrip-long-fuzz.json),
[log](../benchmarks/results/2026-11-06-backup-roundtrip-long-fuzz.log.gz) and
[synthetic corpus](../benchmarks/results/2026-11-06-backup-roundtrip-long-corpus.tar.gz)
are retained. The same serial-model and fault limitations apply.
