# Platform checks for document fields and planned scan budgets

The `ffdd7da` source passed hosted stable and Rust 1.85.0 checks
on macOS and Linux. This revision includes bounded field comparisons
and the planned collection-scan read budget regression. Stable Linux
also passed the optimized suite and 45 standard benchmark smoke cases.

The four-writer Linux recovery workload passed 51 phases, including 24
under the 48 MiB process address-space ceiling. EIO and disposable ENOSPC
checks passed. The [raw archive](raw-artifacts.tar.gz) includes all
49 benchmark attempts, phase data, filesystem fault reports and hosted
job metadata.

[Linux CI](https://github.com/Eta06/EMDA-DB/actions/runs/45356970859) ·
[macOS correctness](https://github.com/Eta06/EMDA-DB/actions/runs/45356970820) ·
[Cross-engine smoke](https://github.com/Eta06/EMDA-DB/actions/runs/45356970999) ·
[Filesystem EIO](https://github.com/Eta06/EMDA-DB/actions/runs/45356971282) ·
[Disposable ENOSPC](https://github.com/Eta06/EMDA-DB/actions/runs/45356970879)
