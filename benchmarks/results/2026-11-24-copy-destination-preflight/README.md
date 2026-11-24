# Destination admission for backup and recovery copies

Revision `ac87adf7` checks destination availability before full-log backup,
restore and format upgrade read and copy payloads. Retained backup and new
catalog preparation use the same check. A file, directory, or dangling
symlink already at the destination returns `NotPublished(AlreadyExists)`.
A missing or unusable parent also rejects before payload work. Atomic
no-clobber publication still protects a path created after admission.

The regression holds the staging parent lock and uses a missing restore
source to prove the occupied path takes precedence. The previous revision
reaches the staging lock and returns `WouldBlock`. Tests also create a
competitor path immediately before backup, restore and upgrade publication.
The error remains `OutcomeUnknown` and the competitor's content survives.

Authorized retained restore skips the independently retained receipt callback
on early rejection. A late conflict still returns the exact receipt prepared
before the rename attempt, while preserving the winning directory contents.
Registration rejects that receipt against a different prepared catalog.

## Cross-platform verification

Native macOS Rust 1.85 passed 1,366 tests across 112 nonempty binaries. [Hosted macOS] passed the
full suites on both toolchains. [Hosted Linux] passed Rust 1.85, stable,
and the stable optimized suite. Their initial runs failed two upgrade
tests whose occupied-path assertions still used the old error category.
Revision `d4402cef` updates just those tests. [Filesystem EIO], [ENOSPC],
and [cross-engine smoke] passed on the unchanged production source in
`ac87adf7`.

The [raw archive] preserves all 52 benchmark attempts, including the initial
million-key run and five cross-engine comparisons. The hosted million-key recovery workload verified
53 phases, with 25 under a 48 MiB address-space ceiling. Native and hosted
logs, the previous-revision regression, verification scripts, and a SHA-256
manifest are included.

[hosted Linux]: https://github.com/Eta06/EMDA-DB/actions/runs/45449691940
[hosted macOS]: https://github.com/Eta06/EMDA-DB/actions/runs/45449691863
[filesystem EIO]: https://github.com/Eta06/EMDA-DB/actions/runs/45448737998
[ENOSPC]: https://github.com/Eta06/EMDA-DB/actions/runs/45448738382
[cross-engine smoke]: https://github.com/Eta06/EMDA-DB/actions/runs/45448738115
[raw archive]: raw-artifacts.tar.gz
