# Planned candidate-count admission regression

The [raw archive](raw-artifacts.tar.gz) preserves macOS ARM64 logs from
Rust 1.85.0. All five file hashes verified after extraction.

The regression uses a two-candidate index range and allows one document
visit. Five logical read units cover collection metadata, the page request,
two index rows, and one document fetch. The baseline `dd6136e` was built
from a clean source extraction with only the `0dff313` test file overlaid.
It failed because the lookahead attempted a sixth logical read.

At `0dff313`, planned query and aggregation both returned the candidate
limit error within five units on ordinary, checkpoint, managed retained,
and authorized retained transactions. All 70 selected tests and
warnings-denied Clippy passed.
