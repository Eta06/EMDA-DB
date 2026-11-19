# New-history retained restore with authenticated file locations

Restore verifies the detached suffix and the base against the independent
binding. It replays ordered records into a bounded index of live keys,
lengths and per-value digests. Input staging limits still govern every
intermediate logical live set. The restore writes one authenticated value
at a time, reopens the staged ordinary store and checks every restored
key, length and value digest before no-clobber publication.

## Verification

The final commit passed 560 library tests and nine selected integration
tests on macOS ARM64 with Rust 1.100.0. Formatting and warnings-denied
Clippy passed. Linux ARM64 with Rust 1.85.0 passed the same nine
integration tests against a byte-matched source tree. A broader
Linux library build was stopped under VM memory pressure while
compiling the earlier implementation.

The tests cover exact and one-under intermediate batch limits, input
digests, empty and maximum-size values, and restore publication and
process-kill outcomes. The mutation hook changes live base and suffix
value bytes after replay. Both cases fail before publication and
clean up the private stage. The existing physical power-loss and
off-host restore deferrals still apply.

## Paired macOS measurement

Both builds used the same base with four 16 MiB values and the same
suffix with four more. There were three alternating pairs of fresh
processes on an Apple M6. Compilation, fixture generation and the
external verification were outside the timed processes. The measurement
includes the complete restore call, including private validation,
synchronized writes, staged reopen and publication. Each output was
reopened independently and all eight values were checked. Input hashes
matched before and after sampling.

| Build | Median peak RSS | Median restore time |
| --- | ---: | ---: |
| Materialized restore `03f64db` | 194.19 MiB | 400.47 ms |
| Location-based restore `5e9517e` | 34.17 MiB | 1,114.46 ms |

The file-location implementation reduced sampled peak RSS by 160.02
MiB and took 2.78 times as long on this fixture. It authenticates the
suffix during the initial binding check and replay, then authenticates
each live value again when reading it into the output store. The
baseline reads from its materialized copy during that write phase.
The measurements cover one warm-cache workload and process RSS.
Ordinary-store index memory, filesystem cache, returned-value
allocations and concurrent restores have separate costs.

## Reproduction

Export both revisions into scratch directories. Put `restore_memory.rs`
under `examples` and build each with
`cargo build --release --locked --example restore_memory`. Invoke the
baseline binary with `setup /path/to/new-fixture unused`. Then run the
two binaries in alternating order under `/usr/bin/time -l`, each with
`restore /path/to/fixture /path/to/new-destination`. Verify each output
with `verify /path/to/fixture /path/to/destination`. The six timing and
verification logs are in `raw-samples.tar.gz`.
