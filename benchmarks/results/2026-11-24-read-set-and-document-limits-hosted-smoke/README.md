# Checkpoint read-set and document limits hosted smoke runs

The [raw archive](raw-artifacts.tar.gz) preserves all 45 successful
Linux CI benchmark smoke cases at `7a36cea`. Both Rust toolchain
jobs passed. The stable job recorded the benchmark cases, including
SQLite controls. [Linux CI](https://github.com/Eta06/EMDA-DB/actions/runs/45336308699).

The archive also contains all three successful cross-engine smoke
cases at `0da8abfb`, covering compound ranges, document contention
and crash workloads. [Cross-engine CI](https://github.com/Eta06/EMDA-DB/actions/runs/45336832788).

The raw files include commands, outputs, exit statuses and workflow
metadata. File hashes were verified after extraction.
