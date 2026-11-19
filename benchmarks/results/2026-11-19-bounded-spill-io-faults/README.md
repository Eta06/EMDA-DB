# Bounded rebuild spill I/O faults

Bounded rebuild now copies exactly the declared number of offset bytes
and rejects an oversized offset sidecar. Exact reads retry interruptions.
A regression test keeps the unlinked run open to measure how many bytes
were written on failure. The old code exceeds the table boundary in that
test. The fixed code rejects the sidecar, stays within the boundary, and
releases both private files and descriptor permits.

## Fault model

The probe uses the public detailed bounded rebuild API. Each fixture
starts with a managed checkpoint and a suffix containing nine ordered
batch children. They include overwrites, tombstones and an empty value.
A one-key chunk and fan-in of two force run creation, random lookups,
multi-pass merge and final emission. V2 and V3 are each tested with an
acknowledged suffix and with the same complete suffix beyond the witness.

On Linux ARM64 ext4, a successful `strace -yy` control identifies each
successful pre-publication syscall that accesses a spill run or its
offset sidecar. Each fault case injects one error at its original
syscall ordinal on a fresh fixture. The runner confirms one injection,
its errno, the expected syscall and the same run or sidecar name.
The archived report records the injected line and the returned outcome.

The fixed path exercised 643 positions per fixture for each error.
Each set covered 82 opens, 77 writes, 17 seeks, 17 sidecar reads, 82
metadata calls, 33 unlinks and 335 positional reads.

| Injected error | Cases | Published after retrying the syscall | Cleanly rejected |
| --- | ---: | ---: | ---: |
| EIO | 2,572 | 0 | 2,572 |
| EINTR | 2,572 | 2,044 | 528 |

All 68 interrupted sidecar data reads completed on the fixed path.
Before the change, all 136 interrupted sidecar reads aborted the rebuild.
That earlier total includes 68 EOF reads that the exact-size copy
eliminates. The completed EINTR cases also include opens, writes and
positional reads. The remaining interruptions return the classified
error.

Each rejection preserved the log bytes, caller-trusted base and
original selection and left the checkpoint destination absent. The
runner also checked the V3 identity anchor and the precise observed
witness in either format. In 24 EIO cases, failure followed adoption
of a complete unacknowledged batch. The earlier selection remained
intact and the witness matched exactly the accepted tail boundary.
Every rejected case cleaned up its owned stages, then passed a clean
retry. Every publication, including a clean retry, matched all
expected values and the full-log checkpoint bytes.

## Platform controls

- macOS ARM64, Rust 1.100.0. Both V2 probe controls verified all values
  and matched the reference checkpoint. The 37 matching library tests
  and nine bounded rebuild integration tests passed. Formatting and
  warnings-denied Clippy passed.
- Linux ARM64, Rust 1.85.0. The 45 matching library tests and 10 bounded
  rebuild integration tests passed at 16 threads. These include the
  large-suffix and seeded differential tests. The compile and test
  inputs matched the macOS checkout.

This campaign models one syscall error before checkpoint publication
on the recorded fixture. Multi-fault sequences, silent partial writes,
cleanup failures after publication and physical persistence failures
fall outside it. Physical power-loss and independent off-host restore
remain deferred.

## Reproduction

Copy `spill_io_probe.rs` to `examples` in a checkout of `118d4be` and
run `cargo build --locked --example spill_io_probe`. On Linux, run
`python3 spill_io_campaign.py PROBE WORKSPACE NEW_REPORT` for EIO.
Use a writable workspace whose filesystem supports V3 xattrs.
Add `--error EINTR --require-retry read --require-retry pread64
--require-retry write` for the interruption campaign. Both runs need
an unused report directory. To reproduce just the interruption defect
before the fix, rebuild at `1c776bc` and run with
`--error EINTR --syscall read`. The raw controls and verified injection
records are in `linux-campaign.tar.gz`.
