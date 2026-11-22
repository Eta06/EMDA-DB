# Bounded planned document window

On macOS ARM64, each fresh process opened the ordinary or checkpoint-backed
store, selected a secondary range index, and visited 192 documents with
24 MiB of string payload. The query sorted descending by the indexed
integer, skipped three matches, projected that field, and returned five
rows. All 32 runs checked the access path, returned IDs and values, and the
scan, match, and return counters.

| Backend | Baseline peak RSS | Streaming peak RSS | Baseline query time | Streaming query time |
| --- | ---: | ---: | ---: | ---: |
| Ordinary | 27.03 MiB | 3.98 MiB | 6.45 ms | 6.15 ms |
| Checkpoint | 27.11 MiB | 4.02 MiB | 9.84 ms | 8.23 ms |

Each value is a median of eight runs per build and backend with alternating
order. RSS includes open. Query time excludes it. These were warm-cache
runs on one host, while its Linux validation VM was active. The probe
measures a scalar-filter query. Array-walking filters may reread candidates
when index order differs from ID order. The probe does not measure that
path or establish a total-memory ceiling.

The [raw archive](raw-results.tar.gz) includes the baseline revision
`4d0071b`, the candidate diff, raw logs, compiler version, binary hashes,
and the runner. Generate the initial store with the
[count and sum probe](../2026-11-22-document-count-sum-streaming/README.md).
Copy this archive's `probe.rs` to
`examples/__emda_planned_window_probe.rs` temporarily in a disposable
checkout. Build with
`cargo build --release --locked --example __emda_planned_window_probe`
at the baseline and again after applying `planned-window.diff`. Place
the binaries at `baseline` and `candidate` in a fresh run directory.
Run `baseline setup <fixture-directory>` once to add the index and a
checkpoint, followed by `sh paired.sh <run-directory> <fixture-directory>`.
