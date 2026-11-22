# Bounded sorted document window

On macOS ARM64, each fresh process opened either the ordinary or
checkpoint-backed store and sorted the same 192 documents containing
24 MiB of string payload. The query sorted by a distinct integer field
descending, skipped three matches, projected that field, and returned
five rows. All 32 runs verified the returned IDs, projected values, and
scan, match, and return counters.

| Backend | Baseline peak RSS | Windowed peak RSS | Baseline query time | Windowed query time |
| --- | ---: | ---: | ---: | ---: |
| Ordinary | 27.94 MiB | 10.47 MiB | 6.24 ms | 5.46 ms |
| Checkpoint | 27.94 MiB | 10.53 MiB | 5.91 ms | 6.01 ms |

Each value is a median of eight runs per build and backend with alternating
order. RSS includes open. Query time excludes it. The filesystem cache
was warm and the Linux validation VM was active on the same host. This
fixture measures a small sorted page from large documents. The scan
page, retained matches, projection and transaction still consume memory.
The planned-query path may also collect index candidates before the sort.

The [raw archive](raw-results.tar.gz) records baseline `24fb4d6`, the
candidate diff, sample logs, binary hashes, and the reproduction runner.
Create the fixture using the [count and sum probe](../2026-11-22-document-count-sum-streaming/README.md).
Copy this archive's `probe.rs` temporarily to
`examples/__emda_sorted_window_probe.rs` in a disposable checkout and
build with `cargo build --release --locked --example __emda_sorted_window_probe`.
Build at the baseline and again after applying `sorted-window.diff`.
Place the binaries at `baseline` and `candidate` in a fresh directory and
run `sh paired.sh <run-directory> <fixture-directory>`.
