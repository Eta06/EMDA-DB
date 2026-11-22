# Retained base recheck during rotation

Reclamation and final rotation checks reread the pinned base. This
experiment hashes the entire previously admitted range without parsing
entry framing a second time. It verifies the original digest, trailer,
and EOF. Fresh opens and staged successors still parse the whole base.

Six alternating pairs per platform ran an epoch of 4,096 operations in
32 durable batches. Each trial started from a fresh copy of the same
131,072-key fixture after six epochs and used the 60-million suffix work
budget. All 24 trials rotated once and verified all 130,700 live keys and
values after reopening. Both platforms ran ARM64. macOS used APFS and the
Linux guest ran on ext4.

| Platform | Baseline CPU median | Candidate CPU median | Baseline wall median | Candidate wall median |
| --- | ---: | ---: | ---: | ---: |
| macOS | 170.48 ms | 159.25 ms | 1,697.38 ms | 1,996.16 ms |
| Linux | 150.59 ms | 143.16 ms | 2,705.19 ms | 2,732.97 ms |

In each platform's six pairs, four candidate CPU times were lower. The
median paired change was −22.09 ms on macOS and −3.18 ms on Linux. Wall
times varied substantially within each platform. Development tests and
builds overlapped these runs on the same host. These samples show a lower
CPU median and leave wall-time impact inconclusive.

The [raw archive](raw-results.tar.gz) includes the baseline revision
`f4b8978`, the candidate source diff, binary hashes, platform and compiler
information, the fixture manifest, the paired runner, and raw logs.
The [benchmark recipe](../../../examples/authorized_recovery_bench.md)
describes fixture setup. Run six completed epochs before cloning the
retained and authority directories for the paired seventh epoch.

On macOS, 161 retained library tests and all eight indexed-candidate
integration tests passed. Linux Rust 1.85 passed all 762 library tests
and the eight indexed-candidate tests with 16 test threads. The first
Linux test build was killed while compiling with two build jobs. The
single-job build completed and both test binaries passed.
