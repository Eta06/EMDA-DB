# Planned collection scan metadata read budget

With the strengthened regression, `34e70bf` exceeds the transaction
read-unit limit on a planned fallback. Reusing the collection schema
already read by the planner makes both planned and direct queries
and aggregations succeed with the same five read units.

All 30 focused tests passed on macOS ARM64 with Rust 1.85.0. The
shared regression covers ordinary, checkpoint-backed, managed retained
and authorized retained writes. Warnings-denied Clippy and formatting
passed. The [raw archive](raw-artifacts.tar.gz) preserves the negative
control, the applied diff and the successful checks. These results
measure logical read units.
