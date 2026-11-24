# Buffer reuse in bounded checkpoint rebuild

The probe compared `8d41c7f` with the bounded rebuild patch using
Rust 1.85.0 release binaries, separate build targets and local macOS
ARM64 APFS fixtures. Four pairs per workload alternated execution order.

Each fixture recorded three passes over 4,096 keys in 256-operation
batches. Full rebuild replayed all 12,288 operations. Suffix rebuild
used the trusted checkpoint from the first pass and replayed the next
8,192 operations. Both used 64-key chunks, four-way merges and a 4 MiB
spill limit. Every run compared the rebuilt checkpoint byte-for-byte
with the ordinary checkpoint, checked the source log hash and verified
every expected key through a managed reopen.

| Rebuild | Allocation requests before → after | Requested bytes before → after | Median seconds before → after |
| --- | ---: | ---: | ---: |
| Full | 36,451 → 32,356 | 9,051,599 → 9,018,839 | 5.445 → 5.304 |
| Suffix | 29,802 → 21,612 | 132,557,359 → 6,128,303 | 1.459 → 1.171 |

Allocation totals repeated across all pairs. The allocator counted
cumulative Rust requests, including reallocations. The totals do not
measure peak heap use or RSS. The full macOS test suite ran concurrently
with this probe, and timing ranges overlap for both workloads.

The [raw archive](raw-artifacts.tar.gz) contains all 16 timed attempts,
the two rejected probe builds, probe source, build logs, distinct
binary hashes, environment and the source patch. To reproduce, copy
`rebuild_buffer_probe.rs` into `examples/` at the baseline revision,
then build `--release --locked --example rebuild_buffer_probe` with
Rust 1.85.0. Apply `source.diff` in a separate checkout and use a
distinct target directory for that build. Run each executable with an
existing disposable fixture parent and mode `full` or `suffix`.

The [Linux CI archive](hosted-linux-smoke.tar.gz) preserves all 45 smoke
cases from the successful stable job at `83715b0`. The overall run
failed Rust 1.85 lint, which was corrected separately. The
[cross-engine archive](hosted-cross-engine.tar.gz) preserves all three
smoke cases at `8d41c7f`. [Linux CI](https://github.com/Eta06/EMDA-DB/actions/runs/45331937448) · [Cross-engine CI](https://github.com/Eta06/EMDA-DB/actions/runs/45333429662).
