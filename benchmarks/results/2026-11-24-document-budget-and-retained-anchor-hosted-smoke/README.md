# Document budget and retained anchor hosted smoke runs

The [raw archive](raw-artifacts.tar.gz) preserves all 45 Linux CI
benchmark cases at `494e73b`. Both Rust toolchains passed. The stable job
recorded the benchmark cases, including SQLite controls.
[Linux CI](https://github.com/Eta06/EMDA-DB/actions/runs/45339798052).

It also contains all three cross-engine cases at `ffdb75d`, covering
compound ranges, document contention and process crashes.
[Cross-engine CI](https://github.com/Eta06/EMDA-DB/actions/runs/45341009803).

Every attempt succeeded. The archive includes commands, outputs, exit
statuses and available raw samples. All embedded file hashes were
verified after extraction.
