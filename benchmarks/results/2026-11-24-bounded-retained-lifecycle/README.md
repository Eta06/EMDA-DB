# Bounded retained backup and restore workload

The lifecycle extends the million-key recovery gate with a verified backup,
disposable legacy archive retirement, receipt-backed restore, and
registration with a fresh authority. It uses explicit logical copy and
verification ceilings for backup, restore, registration and restored
reopen. The final phase compares every live key and value in the source
and restored catalogs with the workload model.

## macOS harness validation

At the engine revision `dc380065`, the harness change in `0571bb6d`
completed all 17 phases on macOS ARM64 using 32,768 initial keys and four
writers. The source and restored scans each checked 29,790 final entries.
The lifecycle had a 10,862,592-byte peak RSS. It used an 88 MiB logical
read ceiling for each copy operation and a 44 MiB ceiling for registration
and restored reopen. The macOS process ran uncapped and this was a smoke
workload.

The [raw macOS archive](macos-artifacts.tar.gz) includes the harness patch,
binary hash, command, phase logs, JSONL measurements and CI verifier checks.
The [benchmark guide](../../../examples/authorized_recovery_bench.md)
describes the million-key Linux configuration. The Linux gate also caps
the lifecycle worker at 48 MiB of virtual address space. Fixture setup,
archive binding and initial cutover run uncapped.

## Hosted Linux million-key restore

The [Linux memory job] at `ac270a31` passed all 53 phases with
1,048,576 initial keys, four writers and four epochs. The 25 capped
phases include backup, archive retirement, recorded restore and
registration in one worker with a hard and soft 48 MiB address-space
ceiling. That worker verified all 1,045,597 live entries in both the
source and restored catalogs and reached a peak RSS of 11,190,272 bytes.

The final worker used a 280 MiB logical read ceiling for each backup
and restore and a 140 MiB ceiling for registration and restored reopen.
On that hosted run, backup took 174.66 ms, archive retirement 276.70 ms,
and restore through registration 460.40 ms. The [Linux raw archive]
retains the exact command, job log, binary identity, all phase logs,
JSONL, and the CI verifier. Filesystem cache and other processes have
separate memory costs. The Linux lifecycle still verifies the legacy
archive as part of the same capped worker.

[Linux memory job]: https://github.com/Eta06/EMDA-DB/actions/runs/45397265214/job/138460536278
[Linux raw archive]: linux-memory-artifacts.tar.gz
