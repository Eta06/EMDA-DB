# macOS concurrent writable recovery smoke

Both finalized attempts passed at source revision `9d823a3` with Rust 1.85.0.
Each used 4,096 initial keys, two mutation epochs and all four backends.
Every epoch verified the complete key/value state after reopening in a fresh
process. Each mutation phase performed 4,096 operations in 32 batches.

| Backend | One writer, median mutation ms | Four writers, median mutation ms |
| --- | ---: | ---: |
| Ordinary | 199.64 | 205.48 |
| Checkpoint | 209.93 | 214.55 |
| Authorized retained | 1,434.90 | 1,428.52 |
| SQLite WAL with synchronous FULL | 5.53 | 28.62 |

These are two samples per cell on one macOS development host. Timings include
worker startup in the concurrent case. Retained writers share one authority
owner behind a mutex. SQLite opens one connection per worker. This smoke run
uses no hard address-space ceiling.

The [raw archive](raw-artifacts.tar.gz) includes both finalized attempt
records, every phase log and measurement, a preliminary four-writer attempt
that predates the coordinated start, the tested patch, and an unsuccessful
Linux VM build attempt. That VM build was killed before a benchmark ran.
The four-writer Linux ceiling is tracked in the
[hosted run](https://github.com/Eta06/EMDA-DB/actions/runs/45351880433).
