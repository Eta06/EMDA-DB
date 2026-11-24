# Bounded rebuild and integer planner hosted smoke runs

The [raw archive](raw-artifacts.tar.gz) preserves all 45 successful Linux
CI benchmark smoke attempts at `ed89743`. Both Rust toolchain jobs passed.
The stable job recorded the benchmark cases, including SQLite controls.
[Linux CI](https://github.com/Eta06/EMDA-DB/actions/runs/45334387319).

It also preserves all three successful cross-engine smoke attempts at
`8edc546`, covering compound ranges, document contention and crash
workloads. [Cross-engine CI](https://github.com/Eta06/EMDA-DB/actions/runs/45335155899).

The archive includes commands, raw output, exit statuses and workflow
metadata. File hashes were checked after extracting it.
