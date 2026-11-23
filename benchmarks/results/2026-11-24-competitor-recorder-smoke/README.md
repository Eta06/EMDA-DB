# Competitor benchmark recorder smoke run

This macOS ARM64 APFS run exercised the recorder with ordinary EMDA,
authority-backed retained EMDA, and SQLite 3.49.1 WAL/FULL. It used the
release binary built with Rust 1.85.0 from `4c46c9c`. Each engine started
with 4,096 keys and ran 4,096 mutations in each of six epochs. Each probe
checked the first value after reopen, and every complete ordered scan
matched the workload model. The retained catalog rotated four times.
The source and the authorized local restore each verified 3,724 final keys.

| Engine | Median wall time through first checked read | Full-state comparisons |
| --- | ---: | ---: |
| Ordinary EMDA | 11.61 ms | 6 |
| Retained EMDA | 9.73 ms | 6 |
| SQLite | 1.33 ms | 6 |

This is a small, warm-cache instrumentation check. The retained open
performs full base verification, while SQLite performs less integrity
work at startup. The [raw archive](raw-results.tar.gz) contains the
attempt record, unmodified command output, environment, individual phase
logs, and all 58 measurement records with their exit status. Reproduce
with the [benchmark recorder](../../../docs/BENCHMARKS.md#competitor-benchmark-records)
and the command recorded in `attempt-01/attempt.json`, updating the
paths for the new attempt.
