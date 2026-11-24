# Managed rebuild source extent at publication

Revision `5eb5602b` carries the rebuilt source extent into the verification
that runs after checkpoint publication and before managed selection. Rebuild
checks the reopened source length and witnessed boundary before opening the
published checkpoint. It authenticates the checkpoint and hashes the complete
rebuilt log extent on the admitted path. It checks the length again around the
final hash and verifies the witness before selecting the checkpoint.

The spill-backed paths also hold the exact extent admitted by either a
physical log length limit or a cumulative source-read reservation. Their
existing source-read reservation and trusted-directory requirements apply.

## Regression coverage

The publication hook injects either a log extension or a changed witness.
It then removes the just-published checkpoint. Both source errors take
precedence over opening the missing file, the original selection and trusted
checkpoint stay intact, and the error classifies checkpoint publication as
uncertain. The test covers ordinary, full spill and trusted-checkpoint spill
rebuilds. The spill cases use exact length and source-read reservations.
The previous revision with the new test reports the missing checkpoint first.
Existing corruption cases continue to reject a changed checkpoint or log
prefix when the extent and witness pass admission.

## Cross-platform verification

The native macOS Rust 1.85 suite passed 1,362 tests across 112 nonempty
binaries. [Hosted macOS] passed both toolchains. [Hosted Linux] passed Rust
1.85, stable, and the stable optimized suite. [Filesystem EIO], [ENOSPC],
and [cross-engine smoke] passed.

The [raw archive] preserves all 51 benchmark attempts, including five
cross-engine comparisons. The hosted million-key recovery workload verified
53 phases, with 25 under a 48 MiB address-space ceiling. Native and hosted
logs, the baseline regression log, verification scripts, and a SHA-256
manifest are included.

[hosted Linux]: https://github.com/Eta06/EMDA-DB/actions/runs/45443519854
[hosted macOS]: https://github.com/Eta06/EMDA-DB/actions/runs/45443519852
[filesystem EIO]: https://github.com/Eta06/EMDA-DB/actions/runs/45443520079
[ENOSPC]: https://github.com/Eta06/EMDA-DB/actions/runs/45443519951
[cross-engine smoke]: https://github.com/Eta06/EMDA-DB/actions/runs/45443519834
[raw archive]: raw-artifacts.tar.gz
