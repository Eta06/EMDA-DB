# Source-read budget across an online reclamation check

Revision `e4197993` gives each automatic worker check a single logical
source-read allowance. Scoped retired cleanup spends from that allowance
only when it verifies the current store. If compaction follows, it spends
from the balance. Each direct cleanup or compaction call starts its own
allowance. The budget resets on the next worker check.

The Linux regression uses one live key. For source log length **L** and
value length **V**, a check without an abandoned retired stage admits
exactly **2L + 2V**. With a matching retired stage it admits exactly
**3L + 2V**. One byte less in the second case still permits verified
cleanup and prevents compaction publication. The test checks the
original log bytes and identity, the acknowledged value, cleanup counters,
and the absence of a published sibling. A subsequent direct compaction
succeeds under its fresh allowance. This measures logical source-read
admission. Target reads, physical retries, and wall-clock duration have
separate costs.

## Cross-platform verification

The native macOS Rust 1.85 suite passed 1,359 tests across 112 nonempty
binaries. [Hosted macOS] passed both toolchains. [Hosted Linux] passed
the regression on Rust 1.85, stable, and the stable optimized build.
The [filesystem EIO], [ENOSPC], and [cross-engine smoke] runs passed.

The archive records 45 CI benchmark smoke attempts, five cross-engine
attempts, and one Linux million-key recovery workload. All 51 succeeded.
The million-key workload verified 53 phases, with a 48 MiB address-space
ceiling in 25 phases.

The [raw archive] preserves the native and hosted logs, individual
benchmark attempts, verification scripts, and a SHA-256 manifest.

[hosted Linux]: https://github.com/Eta06/EMDA-DB/actions/runs/45435026063
[hosted macOS]: https://github.com/Eta06/EMDA-DB/actions/runs/45435025954
[filesystem EIO]: https://github.com/Eta06/EMDA-DB/actions/runs/45435025644
[ENOSPC]: https://github.com/Eta06/EMDA-DB/actions/runs/45435025660
[cross-engine smoke]: https://github.com/Eta06/EMDA-DB/actions/runs/45435026027
[raw archive]: raw-artifacts.tar.gz
