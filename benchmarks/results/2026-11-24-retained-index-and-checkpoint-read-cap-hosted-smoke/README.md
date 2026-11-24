# Retained index and checkpoint read-cap hosted smoke runs

The [raw archive](raw-artifacts.tar.gz) preserves 45 Linux CI cases at
`ffdb75d`. Both Linux toolchains passed. The stable job ran the benchmark
cases, including SQLite controls. [Linux CI](https://github.com/Eta06/EMDA-DB/actions/runs/45341009935).

It also contains all three successful cross-engine cases at `7ed2a70`,
covering compound ranges, document contention and process crashes.
[Cross-engine CI](https://github.com/Eta06/EMDA-DB/actions/runs/45341795506).

Recorded commands, exit statuses, outputs and available samples are
included. All embedded file hashes were checked after extraction.
