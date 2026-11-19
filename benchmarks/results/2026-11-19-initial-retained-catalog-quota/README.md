# Initial retained catalog storage admission

Engine revision `69ac148226e2a663c5e2e702896186d97e9d1a08` admits
the first managed or authority-backed generation under its persistent
logical storage policy. It reserves catalog metadata and the exact bound
suffix before copying the base. Original-path cutover also charges the
current tree, archive hard links and peak fence staging. Private base
export writes one file inside the owned stage and uses the same full
witness and content checks as the public export.

The five new regressions cover exact and one-under bounds, import of a
nonempty suffix from a backup, source preservation after a publication
sync error, initial policy binding to independent authority, and cleanup
of an unfenced preparation after the ordinary source outgrows its
proposed quota.

The ARM64 hosts matched all 313 source and test inputs by SHA-256. The
library suites ran at 16 threads, as did the previous persistent quota
validation. The Linux VM used Rust 1.85.0, offline builds and disabled
incremental compilation and test debug info. macOS used Rust 1.100.0.
Formatting and warnings-denied Clippy passed on macOS. See the
[source manifest](source.sha256).

| Platform | Library tests | Selected integration tests |
| --- | ---: | ---: |
| macOS ARM64 | 554 passed | 58 passed |
| Linux ARM64 | 716 passed | 58 passed |

The integration selection includes retained indexing, retained document
transactions, format guards, and 46 planner, membership, array update and
maintenance interleaving tests. Linux also passed the six-test kernel
file-size suite, which exercises a matrix of 54 child-process failure
cases. The compressed logs contain the per-test results.

These are logical limits within one catalog tree. A separate catalog's
parent markers, physical filesystem allocation, independently placed
authority and backup files, and process memory have separate budgets.
The kernel limit tests exercise process termination and errors based on
file size under a live OS. Physical power loss and independent off-host
restore remain deferred.
