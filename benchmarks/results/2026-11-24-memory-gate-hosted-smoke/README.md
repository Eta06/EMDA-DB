# Linux smoke cases after adding the retained memory gate

Both Linux toolchains passed at `38e8988`. Rust stable also passed the
optimized test suite and all 45 smoke cases. The
[raw archive](raw-artifacts.tar.gz) preserves commands, outputs, structured
samples and hosted job metadata for all 45 cases. All 114 file hashes
verified after extraction.

The [separate retained memory gate archive](../2026-11-24-retained-linux-memory-ceiling/README.md)
records the capped workload and its ordinary and SQLite controls from
the same run. [Hosted CI](https://github.com/Eta06/EMDA-DB/actions/runs/45344487724).
