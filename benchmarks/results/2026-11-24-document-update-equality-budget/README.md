# Bounded document update equality

Revision `2e47a4d` shares one exact JSON equality budget across matching
preconditions, array add-unique, and array remove. It charges nested
value comparisons and logical string and object-key bytes. The query
path uses the same comparator and keeps its existing counters and error
classification.

On ordinary and checkpoint stores, the first regression spends exactly
1,000,000 nested comparisons across conditions and add-unique. A later
operator exceeding that ceiling rolls back both the update and a staged
raw write. The second regression spends the shared text cap across
repeated removals and checks that the over-budget call leaves the
document unchanged.

Native macOS Rust 1.85.0 passed 1,338 tests across 112 nonempty binaries.
Formatting, warnings-denied Clippy and rustdoc passed. Hosted [macOS] and
[Linux] passed both Rust toolchains, including stable optimized Linux.
The [filesystem fault] and [ENOSPC] workflows passed. The [raw archive]
contains all 45 Linux and five [cross-engine smoke] attempts, as well as
the million-key recovery memory workload and the verification script.

[raw archive]: raw-artifacts.tar.gz
[macOS]: https://github.com/Eta06/EMDA-DB/actions/runs/45393960416
[Linux]: https://github.com/Eta06/EMDA-DB/actions/runs/45393960342
[filesystem fault]: https://github.com/Eta06/EMDA-DB/actions/runs/45393960489
[ENOSPC]: https://github.com/Eta06/EMDA-DB/actions/runs/45393960396
[cross-engine smoke]: https://github.com/Eta06/EMDA-DB/actions/runs/45393960420
