# Covered-log reads during bounded checkpoint rebuild

Bounded rebuild continues to verify the independently trusted base checkpoint
and its covered log before suffix replay. After replay it verifies the
checkpoint file again, then checks the covered log and replayed suffix in one
stream before advancing the witness. It verifies the base checkpoint file again
before streaming its entries. Staged checkpoint verification and the reopen of
the published checkpoint still check the covered log bytes.

In `d4373c0`, the two intermediate checkpoint-file checks also hashed the
covered log prefix. The first preceded the `before_adopt` hook. The regression
changes the base checkpoint at that hook. The baseline returned an error after
advancing the witness. The current path rejects the changed checkpoint before
advancing the witness and succeeds after the original checkpoint is restored.

## Linux ARM64 ext4

The existing [spill I/O probe](../2026-11-19-bounded-spill-io-faults/spill_io_probe.rs)
verifies publication against full-log recovery and a byte-identical reference
checkpoint. The [evidence archive](linux-traces-and-faults.tar.gz) includes the
probe version used for this run. The larger fixture adds one overwritten 8 MiB
value before the base checkpoint so the live state and suffix still fit the
same limits. Both binaries used Rust 1.85.0. Each configuration ran on fresh
fixtures with V2 and V3 and with an acknowledged suffix or a complete
unacknowledged tail. All 16 control runs matched the reference.

`strace -yy -s 0 -e trace=pread64` measured successful reads from `records.log`
during the rebuild attempt, including its internal published-checkpoint reopen.
Preparation and separate reference verification were outside the trace. Each
row gives the identical result across all four witness and suffix configurations.

| Covered prefix bytes | Baseline bytes read | Current bytes read | Bytes saved |
| ---: | ---: | ---: | ---: |
| 80 | 1,724 | 1,564 | 160 |
| 8,388,710 | 50,333,504 | 33,556,084 | 16,777,420 |

The saved reads equal twice the covered prefix. These are logical bytes returned
by the kernel. Cache state, device I/O, elapsed rebuild time and memory were
outside this measurement.

The [source-read fault runner](../2026-11-19-checkpoint-replay-binding/replay_io_campaign.py)
injected `EIO` or `EINTR` at each of the 15 prepublication source reads on a
fresh small fixture per fault. All 60 `EIO` cases rejected publication. In 14 of
those cases the witness had already advanced to the complete validated tail.
All 60 `EINTR` cases published. Every publication or clean retry matched the
full-log reference. The archive includes the traces and individual outcomes.

Probe SHA-256 values are `5a83255430833c7ec2877c47250b887ef76271c77978cab9833c1dcf299f1a8e`
for the baseline and `e1aec2985a8f2f4b9ff5e0c17b07df7af56021aadbef4da0afa52caf9890a95a`
for the current build.

The library and two bounded-rebuild integration targets passed on macOS
(584 library and 8 integration tests) and on Linux ARM64 (747 library and
9 integration tests). Formatting and warnings-denied Clippy passed on macOS.
