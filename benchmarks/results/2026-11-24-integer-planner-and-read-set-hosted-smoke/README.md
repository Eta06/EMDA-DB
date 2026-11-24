# Integer planner and checkpoint read-set hosted smoke runs

The [raw archive](raw-artifacts.tar.gz) preserves all 45 successful
Linux CI benchmark smoke cases at `8edc546`. Both Rust toolchains
passed. The stable job recorded all benchmark cases, including SQLite
controls. [Linux CI](https://github.com/Eta06/EMDA-DB/actions/runs/45335155917).

The archive also contains all three successful cross-engine smoke
cases at `7a36cea`. They cover compound ranges, document contention
and crash workloads. [Cross-engine CI](https://github.com/Eta06/EMDA-DB/actions/runs/45336308610).

The raw files include commands, outputs, exit statuses and workflow
metadata. File hashes were verified after extraction.
