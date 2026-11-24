# Checkpoint rollover membership probe

The probe compared `f6265db` with the patch that resolves touched-key
membership during trusted index verification. Each run seeded 16,384
keys and applied the indicated number of distinct suffix mutations.
The measured operation was managed rollover, including verification
and synchronization. Every run reopened the result and checked all
base and suffix keys. Four paired runs per size alternated execution
order on macOS ARM64 using Rust 1.85.0 release builds.

| Touched keys | Allocation requests before → after | Requested bytes before → after | Median rollover µs before → after |
| ---: | ---: | ---: | ---: |
| 0 | 66 → 66 | 94,346 → 94,346 | 30,674.5 → 29,594.5 |
| 64 | 135 → 133 | 186,724 → 121,316 | 32,481.5 → 43,190 |
| 4,096 | 4,174 → 4,172 | 1,208,330 → 1,142,922 | 95,624 → 83,184 |

Allocation totals repeated across all pairs. The 65,408-byte reduction in
both nonempty cases reflects removal of two temporary block-read buffers.
Allocation totals are cumulative requested sizes. Timings varied and
the medians moved in both directions. These runs used local fixtures and
did not measure peak RSS, Linux timing, or competitor performance.

The [raw archive](raw-artifacts.tar.gz) contains all 24 measured attempts,
environment details, build logs, binary hashes, the probe, and the
source patch. The first shared-target build pair produced identical
binaries and was rejected before samples were taken. Its hashes and
build logs are included. To reproduce, place the probe in `examples`
in a baseline checkout, build with
`cargo +1.85.0 build --locked --release --example rollover_alloc_probe`,
apply `source.diff` in a second checkout, and build with a distinct
Cargo target directory. Pass `0`, `64`, or `4096` to each binary.

The [hosted archive](hosted-cross-engine.tar.gz) preserves the three
cross-engine smoke cases from baseline revision `f6265db`, including
commands and raw results. [Hosted run](https://github.com/Eta06/EMDA-DB/actions/runs/45332681582).

The raw archive also preserves the targeted Linux VM build attempt.
The compiler received SIGKILL before the tests started.
