# Streaming document count and sum

The ordinary and checkpoint-backed stores used the same macOS ARM64
APFS fixture with 192 documents, 24 MiB of string payload, and eight
integer groups. A complete snapshot scan counted and summed all
documents. Each run started a fresh process. All 32 runs checked the
query plan counters and every group's count and sum against the
fixture formula.

| Backend | Baseline peak RSS | Streaming peak RSS | Baseline query time | Streaming query time |
| --- | ---: | ---: | ---: | ---: |
| Ordinary | 28.02 MiB | 10.38 MiB | 5.99 ms | 5.02 ms |
| Checkpoint | 28.02 MiB | 10.41 MiB | 5.43 ms | 5.22 ms |

Values are medians across eight runs per build and backend. Query time
excludes open. Peak RSS from `/usr/bin/time -lp` covers the entire
process, including database open. The operating system page cache was
warm and the Linux test VM was active on the same host. The fixture
isolates a query that groups small fields in documents with large
unused payloads. It gives no process memory ceiling or general latency
estimate.

The [raw archive](raw-results.tar.gz) records the baseline `ec92e78`,
the candidate source diff, compiler version, binary hashes and samples.
To repeat on macOS, copy the archived `probe.rs` temporarily to
`examples/__emda_grouped_scan_probe.rs` in a disposable checkout. Build
with `cargo build --release --locked --example __emda_grouped_scan_probe`
at the recorded baseline, then after applying `streaming.diff`. Place
the binaries at `baseline` and `candidate` in a fresh run directory.
Run `baseline setup <run-directory>/fixture` once and then
`sh paired.sh <run-directory>`.
