# Document update input accounting: local comparison

The 4 MiB combined encoded-input limit bounds the logical JSON values supplied to a conditional update, including repeated and ignored values. It changes behavior: inputs that were previously accepted above this limit now fail before conditions are evaluated. The initial implementation counted encoded bytes through `serde_json`; a later safe Rust counter counts strings and nested values exactly while continuing to use `serde_json` for number formatting. Neither implementation changes the storage transaction or its durability rules.

These measurements cover one deliberately narrow case: a single in-process client repeatedly executes a successful `SetIfMissing` for a field that already exists. The input is ignored by the update but must be validated, and both versions synchronize the same unchanged document transaction. Values are below the cap. The results do not measure over-limit rejection, write throughput, concurrency, total memory, other databases, physical power loss or off-host restore.

## Optimizing the exact counter

Median of six per-process medians, in microseconds, from each of two ABBA runs (`ba3cd96`, the first limited implementation, versus `fe8ffa4`, the optimized implementation):

| Supplied JSON value | First counter | Optimized | First counter, repeat | Optimized, repeat |
| --- | ---: | ---: | ---: | ---: |
| 64-byte ASCII string | 5.98 | 5.98 | 6.00 | 5.97 |
| 1 MiB encoded ASCII string | 529.44 | 336.88 | 529.57 | 336.55 |
| 1 MiB encoded escaped string | 356.50 | 221.93 | 356.50 | 221.99 |
| Small nested object | 6.17 | 6.19 | 6.11 | 6.17 |
| Nested object with a large string | 525.37 | 338.38 | 523.36 | 336.52 |
| Array of 1,024 integers | 18.21 | 16.41 | 18.08 | 16.33 |

The large cases improved in these samples; the small-object difference is within the observed noise. A separate [paired run](../benchmarks/results/2026-11-04-update-input-nested-paired.json.gz) compares the string-only counter (`1ae2d60`) to the nested-value counter. It measured the nested large string at 363.16 versus 334.35 µs and the integer array at 20.67 versus 16.13 µs. The [first](../benchmarks/results/2026-11-04-update-input-fast-primary.json.gz) and [repeat](../benchmarks/results/2026-11-04-update-input-fast-repeat.json.gz) contain all samples and noise observations.

## Cost of the new limit

The optimized implementation was also compared with the pre-limit revision `379e269`. The prior implementation did not enforce the new combined limit; for this ignored-value case it did not perform the same complete input accounting. Median of six process medians:

| Supplied JSON value | Before limit | Optimized limit |
| --- | ---: | ---: |
| 64-byte ASCII string | 6.02 µs | 6.01 µs |
| 1 MiB encoded ASCII string | 6.02 µs | 337.02 µs |
| 1 MiB encoded escaped string | 6.06 µs | 223.42 µs |
| Small nested object | 6.18 µs | 6.19 µs |
| Nested object with a large string | 6.05 µs | 337.05 µs |
| Array of 1,024 integers | 8.76 µs | 16.10 µs |

This is a substantial latency cost for large ignored inputs and number-heavy arrays. The [raw comparison](../benchmarks/results/2026-11-04-update-input-budget-optimized-cost.json.gz) preserves every measurement. Earlier comparisons of the pre-limit code with the first counter are also retained: an [exploratory run](../benchmarks/results/2026-11-04-update-input-budget-cost-primary.json.gz) and a [six-scenario run](../benchmarks/results/2026-11-04-update-input-budget-cost-repeat.json.gz). The escaped-string median for the first counter was 589.99 µs in the exploratory run and 356.57 µs in the later run, illustrating why one run is insufficient.

## Method and limitations

The six-scenario runs contain four passes in baseline/candidate/candidate/baseline order, three independent process rounds per pass and 30 timed operations per process after five warmups. Scenario order rotates across passes and rounds. Each process verifies its final document, encoded byte count and sample count. The exploratory three-scenario run used four rounds per pass. Timings include transaction synchronization on a VM tmpfs filesystem; they do not establish storage-device durability or represent a disk-backed service.

The VM reported an Intel Xeon Platinum 8573C and a four-CPU cgroup quota. Benchmark and driver were pinned to separate allowed CPUs. Every pre-run quiet check reported quiet, and the measurement intervals reported no cgroup throttling events. Short interval VM-steal observations reached 33.3%; these coarse whole-VM observations, frequency changes and other shared-host activity limit precision. The retained samples include this noise rather than filtering it out. There were no failed benchmark runs.

The measured [benchmark source](../examples/update_input_bench.rs) has SHA-256 `54d0abde94ded7d5f283313ba4a947fb7a0d5ab56d2d9678495aaab25101889d`; the [driver](../scripts/update_input_compare.py) has SHA-256 `e766444ddc93f7afe6792357b76d19a08b425e8942b31e30455dabdf12920e09`. The earlier exploratory run used the retained [initial benchmark](../benchmarks/experiments/update_input_bench_initial.rs) and [driver](../benchmarks/experiments/update_input_compare_initial.py). Builds used Rust 1.99.1 and the same [standalone harness lockfile](../benchmarks/experiments/update-input-Cargo.lock), SHA-256 `c89c52552b9324b26fac4a7f28e491b168ed0e34442a6b56177ecf86a63b6796`. Revisions, clean worktree status, binary hashes, commands, affinity, raw timings and telemetry are recorded in each result.

For reproduction, build the same benchmark source as a small standalone Cargo binary against separate clean engine worktrees, using the retained lockfile. The measured harness manifest used package `update-input-bench` version `0.0.0`, edition 2024, a `[[bin]]` named `update_input_bench` pointing to the shared source, and dependencies `database-engine` as a path to the selected worktree, `serde_json = "1.0"` and `tempfile = "3.20"`. Build each with `cargo +stable build --release --locked` and a separate `CARGO_TARGET_DIR`, then run `scripts/update_input_compare.py` from the candidate worktree with `--baseline-binary`, `--candidate-binary`, `--baseline-revision`, `--directory`, `--output`, `--rounds 3` and `--cycles 30`. Use `--mode limit` for before/after admission and `--mode counter` for two implementations that enforce the same limit. Repeat on suitable storage and workloads before making broader decisions.

Correctness evidence is separate: [11 model-fuzz campaigns](../benchmarks/results/2026-11-04-update-input-fuzz.json) exercised 43,508 cases across the three revisions. Deterministic tests compare exact and one-byte-over counts with `serde_json`, including every ASCII byte at word offsets, Unicode, escapes, nested JSON and tree boundaries. A [30-run stress record](../benchmarks/results/2026-11-04-update-input-stress.json) covers the initial limit on tmpfs and the workspace overlay. These finite tests do not establish a total-memory limit or prove physical power-loss safety.
