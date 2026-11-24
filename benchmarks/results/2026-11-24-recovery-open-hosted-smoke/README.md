# Recovery open hosted smoke runs

The [raw archive](raw-artifacts.tar.gz) preserves all 45 successful
Linux CI benchmark cases at `0da8abfb`, including SQLite controls.
The Linux matrix passed on stable Rust and Rust 1.85. The stable job
recorded the benchmark cases. [Linux CI](https://github.com/Eta06/EMDA-DB/actions/runs/45336832808).

It also preserves all three successful cross-engine smoke cases at
`1cc179b5`, covering compound range queries, document contention, and
process crashes. [Cross-engine CI](https://github.com/Eta06/EMDA-DB/actions/runs/45337970597).

Each case retains its command, outputs, exit status, and available raw
samples. The file hashes were verified after extraction. These short
workloads exercise the benchmark harness and provide preliminary measurements.
