# Planned index scan and response paging regression

On macOS ARM64 with Rust 1.85.0, the regression failed at `e244077`
with a document transaction read-limit error. It passes with private
index scans using the shared candidate budget. The test verifies both
planned projection and planned aggregation over 17 nearly 1 MiB
documents under 36 read units. An explicit named index still returns
these documents across two pages.

All 71 focused tests passed, along with warnings-denied Clippy and
formatting. This is a read-budget measurement. It records no timing
comparison.

The [raw archive](raw-artifacts.tar.gz) contains the failing baseline,
the applied diff and the successful checks.
