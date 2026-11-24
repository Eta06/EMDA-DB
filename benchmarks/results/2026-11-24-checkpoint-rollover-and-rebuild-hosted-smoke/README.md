# Checkpoint rollover and bounded rebuild hosted smoke runs

The [raw archive](raw-artifacts.tar.gz) preserves all 45 successful
Linux CI benchmark smoke attempts at `8d41c7f`, including SQLite
controls. Both Rust toolchain jobs passed. [Linux CI](https://github.com/Eta06/EMDA-DB/actions/runs/45333429820).

It also preserves all three successful cross-engine smoke attempts
at `ed89743`, covering compound ranges, document contention and
crash workloads. [Cross-engine CI](https://github.com/Eta06/EMDA-DB/actions/runs/45334387294).

Both directories contain commands, output, exit statuses, and workflow
metadata. File hashes were verified after extracting the archive.
The smoke workloads exercise comparability and result collection.
