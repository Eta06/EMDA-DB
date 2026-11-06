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

A subsequent [clean campaign](../benchmarks/results/2026-11-06-cleanup-double-errors.json.gz)
on `415273c` also failed fixture removal calls after an earlier data or
namespace error. It checked 177 primary failures and 982 paired cases: 258
second `fsync` failures, 330 `unlink` failures and 394 `unlinkat` failures,
each with the applicable recovery order. Every paired action returned an
error. After fault injection stopped, recognized top-level staging payloads
were cleaned; 32 cases retained only an unchanged malformed marker without
its payload, which cleanup reported as skipped. The initial [test-oracle run](../benchmarks/results/2026-11-06-cleanup-double-initial-oracle.json.gz)
incorrectly required every marker-shaped entry to disappear; inspection showed
an empty marker created before the first error and preserved after its unlink
also failed. The verifier was corrected to require that such unrecognized
entries stay unchanged and have no payload, while still rejecting leftovers
that cleanup should own. This checks the fixture's top-level staging namespace,
not every nested entry or arbitrary combinations of the same syscall.

The [hosted filesystem workflow](https://github.com/Eta06/EMDA-DB/actions/runs/42157059031)
also passed on revision `9c8f07a`, using a Linux ext4 runner. Its uploaded
artifact records 818 one-shot sync cases, 818 persistent-sync cases, 206 data,
888 read, 682 open, 148 namespace and 990 paired-error cases. Thirty-two paired
cases retained an unchanged malformed marker without its payload. These counts
reflect the calls reached on that runner; they do not establish behavior on
other filesystems, physical power loss or off-host recovery.

### Repeated failures of the same syscall

On clean revision `8a72ba6015b216712c4672ba49f1786dcb1af60a`, the
[expanded paired-error campaign](../benchmarks/results/2026-11-06-double-eio-same-syscall.json.gz)
ran 370 first-error probes and 1,956 paired-error recovery cases. The first
failure targeted selected data, namespace or synchronization calls observed
on a successful fixture path; the second targeted a later synchronization or
fixture-removal call reached on that error path. Both writable recovery paths
took turns opening first. Among the paired cases, 190 injected `fsync` twice
and eight injected `unlink` twice. The other cases used different syscalls.
Thirty-two cases preserved unchanged malformed marker-only remnants, with no
unmarked top-level staging payload; other recognized leftovers were cleaned
after injected errors stopped.

The [hosted filesystem workflow](https://github.com/Eta06/EMDA-DB/actions/runs/42162383288)
also passed the updated checks on ext4: its artifact records 998 paired cases
for data/namespace primaries and 158 for V3 checkpoint-compaction sync
primaries, including 38 same-syscall cases in total. The local workspace
overlay's backing device was not verified. These are selected returned errors
on a running OS, not all multiple-fault combinations or physical power loss.

### Paired failures at publication ancestors

On clean revision `1d9cac377028c4c1ab0911598c8013ebf5838c41`, a
[further campaign](../benchmarks/results/2026-11-06-double-eio-sync-ancestors.json.gz)
selected every `fsync` and `fdatasync` observed on each successful fixture
path, including resolved publication ancestors. Across 26 format/operation
controls, it checked 373 first-error probes and 1,606 paired-error cases;
180 first errors targeted ancestors outside the fixture and 640 paired cases
followed those ancestor errors. The pairs included 410 cases with two `fsync`
errors. For each first error, every later synchronization or fixture-removal
call observed on that failure path was failed in turn with both recovery
orders. The new-store initialization target used the filesystem's default
format in both reference-fixture groups.

The [hosted workflow](https://github.com/Eta06/EMDA-DB/actions/runs/42163760740)
passed its V3 checkpoint-compaction subset on ext4: 28 first-error probes,
including 12 ancestor calls, and 218 paired cases, including 54 with two
`fsync` errors. Its artifact retains the traces. These checks cover calls
observed in the specified traces, not every possible filesystem failure,
arbitrary subsequent failures, physical power loss or off-host restore.

### Atomic batches under returned errors

On clean revision `bc1d325`, a further [recorded campaign](../benchmarks/results/2026-11-06-atomic-batch-eio.json.gz)
injected 248 one-shot or persistent errors into V2/V3 ordinary and
checkpoint-backed batch paths, using both recovery orders. Each batch puts two
new keys and deletes an existing key. All injected actions returned errors;
212 checked cases recovered the prior state and 36 recovered the complete
batch. Neither recovery path accepted a partially visible batch. These are
case counts, including repeated recovery orders, not 248 independent
physical crashes. The verifier has negative controls for partial insertion
and a missing deletion. The two-error driver also checked ten primary failures
for these batch operations, but those failure paths made no later sync calls,
so this particular batch run exercised no second-error cases. Injected
syscalls returned `EIO` without a partial successful write; physical power loss
and other error combinations remain outside the evidence.

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
V2/V3 and ordinary/checkpoint-writer combination. These used the default `/tmp`
temporary directory, identified immediately afterward as tmpfs. In 842 rounds at least one
writer's acknowledgement counter had not reached the current round when the
backup returned; in 643 rounds at least one had reached it. These categories
can overlap, and an outstanding counter does not prove the writer was executing
at that instant. The raw [stress record](../benchmarks/results/2026-11-06-concurrent-backup-stress.json)
contains every run and the observed counters. The first development version of
the test encountered an expected optimistic conflict; the recorded campaign
uses bounded retries for that specific conflict and fails on other errors.

Ten additional optimized repetitions on the workspace overlay completed 480
backup/restore rounds, with 120 in each format/backend combination. In 470
rounds at least one counter was outstanding and in 410 at least one had
completed; these counts can overlap. The [overlay stress record](../benchmarks/results/2026-11-06-concurrent-backup-overlay.json)
includes the fixture location and per-run observations. Its backing device was
not independently identified. Neither filesystem run simulates power loss or
an independent off-host restore.

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
