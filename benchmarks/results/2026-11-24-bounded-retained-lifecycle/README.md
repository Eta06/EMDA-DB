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
