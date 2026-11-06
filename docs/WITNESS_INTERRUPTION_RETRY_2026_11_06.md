# Retrying interrupted V3 witness updates — 2026-11-06

On Linux V3, a synthetic `EINTR` returned by `fsetxattr` during a commit used to
leave the open store failed. Concurrent writers then received operation errors
whose outcomes could be unknown. This was safe but reduced availability for a
transient interruption.

The V3 witness now retries `EINTR` only for an existing witness replacement.
The witness lock remains held and the retry uses the same bytes and flags, so
the replacement is idempotent even if the interrupted call installed it.
Success still requires the following full `fsync`; other errors still propagate
and preserve their unknown-outcome semantics. New-witness `CREATE`, arbitrary
callback errors and synchronization failures are not converted to success.
Repeated interruptions can delay completion; this does not introduce a
deadline or bounded retry guarantee.

## Concurrent before and after

On the shared Linux VM's workspace overlay, two repetitions of each V2/V3,
ordinary/checkpoint writer, witness/log-sync, first/second per-thread ordinal
and recovery-order configuration produced 64 cases per revision. Four writers
attempted up to 64 unique keys each. `strace` verified each injected errno and
target, and both recovery paths checked every acknowledged write and the
identity of any recovered failed write.

| Revision and V3 witness cases | Cases | Acknowledgements | Failed writer calls | All writers succeeded |
| --- | ---: | ---: | ---: | ---: |
| [Before](../benchmarks/results/2026-11-06-witness-eintr-before.json.gz), `5ed3cd9` | 16 | 26 | 64 | 0 |
| [After](../benchmarks/results/2026-11-06-witness-eintr-after.json.gz), `1902efe` | 16 | 4,096 | 0 | 16 |

The before run recorded 16 injected V3 witness interruptions and the after
run recorded 64; scheduling and the number of calls reached differed because
the earlier writer stopped after the first error. The other 48 cases per run
covered V2 witness and V2/V3 log-sync interruptions, with all 12,288 attempted
writes acknowledged and recovered in each run. These are fault-injection
counts, not latency or throughput measurements.

## Later synchronization and non-interruption failures

In 16 [paired-fault cases](../benchmarks/results/2026-11-06-witness-retry-paired-faults.json.gz),
the traced V3 witness replacement first returned `EINTR`, then the identical
replacement succeeded. Both recovery orders checked ordinary and checkpoint
single writes and atomic batches. Eight cases allowed the following log
`fsync` to succeed; all writes succeeded. Eight instead injected `EIO` into
that `fsync`; all eight calls returned errors without acknowledging the write.
Both recovery paths observed the complete attempted writes in those error
cases, which is an allowed unknown outcome and is not a physical durability
claim.

A separate [32-case EIO regression](../benchmarks/results/2026-11-06-witness-eio-after.json.gz)
covered witness and log-sync errors on both formats and writable paths. It
verified every observed acknowledgement and each failed-write outcome after
both recovery paths; the injected errors remained visible to writers.

The checks inject syscall return values, rather than delivering real signals
or simulating a physical power failure. They sample schedules on one shared
VM and do not establish device behavior, off-host restoration, or exhaustive
correctness. CI runs selected interrupted and paired-fault cases on hosted
Linux as well.
