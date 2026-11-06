# Concurrent witness-write failures — 2026-11-06

On clean revision `5cecd1adaa43aae02a186a4c8de418a13519be83`, the
[local campaign](../benchmarks/results/2026-11-06-concurrent-witness-eio.json.gz)
ran four independent writers against V2/V3 stores on both writable backends.
Linux `strace` injected `EIO` into a V2 witness `pwrite64` or a V3 metadata
`fsetxattr`; injection ordinals are counted per traced thread. Each writer
recorded successful returns and stopped after its first error. Both recovery
paths took turns opening first; the verifier checked all observed successful
writes, allowed each failed write to be present or absent, and required both
paths to agree on exactly which failed writes appeared.

Twenty repetitions completed 320 cases. All 428 observed acknowledged writes
were recovered. The workers reported 1,280 failed calls; 524 of those writes
were recovered in full, demonstrating the documented unknown outcome. The
traces showed a multi-record vectored append in 80 ordinary-store cases.
Earlier acknowledged fixture values, the source log prefix, reference backup,
checkpoint and relevant witness data were also checked. A negative-control
test rejects a missing acknowledgement, and a driver test rejects recovery
paths that report different uncertain writes even when their counts match.

All 320 recorded injections were independently checked against the source's
actual V2 `commit.state` descriptor or the V3 `user.db.commit.v3` attribute on
`records.log`. The driver now enforces those exact targets. A subsequent
[48-case clean run](../benchmarks/results/2026-11-06-concurrent-witness-targets.json.gz)
on `41dc3cb` passed with 63 acknowledged writes recovered and 78 failed writes
present after recovery. These case counts include both recovery orders.

## Concurrent log synchronization errors

On clean revision `6663651a1e1ba6359118b1206b20946d5c9558ea`, the
[expanded local campaign](../benchmarks/results/2026-11-06-concurrent-witness-and-sync-eio.json.gz)
also injected an `EIO` into synchronization of the source `records.log`,
using `fdatasync` on V2 and `fsync` on V3. A trace path filter excludes
directory synchronization during opening, and the driver verifies the
injection reached the actual source log. The first or second matching call
per traced thread was selected. The test checks both recovery orders on both
writable backends and uses four independent writers in one process.

| Injected failure | Cases | Observed acknowledged writes recovered | Failed calls | Failed writes recovered |
| --- | ---: | ---: | ---: | ---: |
| Witness write | 320 | 472 | 1,280 | 489 |
| Log synchronization | 320 | 472 | 1,280 | 485 |

Every case recorded an injection at the intended file or metadata attribute.
Both recovery paths agreed on the exact recovered failed-write identities;
each case also checked its prior acknowledged data and reference fixtures.
An ordinary-store multi-record vectored append appeared in 80 cases for each
failure mode. These counts include both recovery orders and reflect sampled
schedules on the workspace overlay, whose underlying device is not verified.
An injected returned error is not a partial successful syscall or a physical
power loss, and a recovered failed write does not mean it was acknowledged.

The [GitHub workflow](https://github.com/Eta06/EMDA-DB/actions/runs/42160313151)
subsequently passed the expanded campaign on ext4 for revision `c06cf81`.
Its retained artifact reports 96 cases, with all 137 observed acknowledged
writes recovered; 141 failed writes were present after recovery. It also ran
the broader filesystem-EIO suites. The hosted run has the same fault-model
limitations as the local run.

The [driver](../scripts/concurrent_eio_campaign.py) records the exact
receipts, injected syscalls, selected vectored appends, hashes and recovery
results. Its [GitHub workflow](../.github/workflows/sync-eio.yml) repeats a
shorter campaign. The test uses threads in one process and returned errors on
a running OS; it does not inject partial successful syscalls, exhaust all
interleavings, simulate physical power loss or perform an off-host restore.

## Concurrent two-key transactions

On clean revision `8520081b656c06bcaf47cccdddda423590d93563`, four writers
used transactions to write unique pairs of keys with matching values. The
verifier requires both halves of each acknowledged or uncertain transaction to
be present together or absent together; it also checks all earlier
acknowledged data and requires the two recovery paths to agree on exactly
which failed transactions are present. The test retries `WouldBlock` only
because these specific writes repeat the same keys and values; an arbitrary
transaction with an operation error must not be assumed safe to retry.

Twenty repetitions of the [EIO campaign](../benchmarks/results/2026-11-06-concurrent-transaction-eio.json.gz)
covered 640 cases across V2/V3, both writable paths, witness/log-sync errors,
first/second per-thread ordinals and both recovery orders. All 1,091 observed
acknowledged transactions were recovered intact. Workers reported 2,560 failed
calls; 640 were present as complete pairs after recovery and 1,920 were
absent. A failed call remains an unknown outcome, not a confirmed abort.

Two repetitions of the separate [EINTR campaign](../benchmarks/results/2026-11-06-concurrent-transaction-eintr.json.gz)
covered 64 cases. All 16,384 attempted transactions succeeded and both values
from each were recovered; 256 injected interruptions were observed at the
selected witness or log-sync calls. Negative controls reject missing
acknowledgements, malformed receipts and fractured uncertain pairs.

Both campaigns inject returned errno values under sampled thread schedules on
the shared overlay. They do not inject partial successful syscalls, kill the
process during commit or establish physical power-loss behavior.

The [independent report verifier](../scripts/verify_concurrent_eio_results.py)
checked the complete case matrix, receipt counts, uncertain-write identities,
injected errno and target, and recorded reference-fixture hashes in both raw
reports. It validates the recorded evidence; the probe performed the actual
pair and source-prefix checks while the fixtures existed.
