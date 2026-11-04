# Transaction cancellation overhead comparison — 2026-11-04

A paired local comparison of transactions that do **not** request cancellation produced mixed results. Some sampled medians were slower after the change and others were faster; the repeat did not reproduce the largest observed slowdown. The measurements do not establish that the added checks are free or identify the cause of the variation.

| Workload / clients / mode | Baseline p50 | Candidate p50 | Baseline p99 | Candidate p99 |
| --- | ---: | ---: | ---: | ---: |
| Disjoint / 1 / read-set, primary | 270.68 | 268.96 | 845.18 | 699.34 |
| Disjoint / 1 / checkpoint read-set, primary | 224.03 | 229.28 | 481.71 | 507.59 |
| Disjoint / 4 / read-set, primary | 285.56 | 269.20 | 13,637.68 | 22,896.46 |
| Disjoint / 4 / checkpoint read-set, primary | 379.13 | 498.10 | 17,462.93 | 10,483.66 |
| Disjoint / 1 / read-set, repeat | 242.02 | 240.51 | 588.99 | 470.71 |
| Disjoint / 1 / checkpoint read-set, repeat | 235.74 | 224.07 | 569.78 | 521.90 |
| Disjoint / 4 / read-set, repeat | 266.56 | 262.53 | 17,636.87 | 14,306.96 |
| Disjoint / 4 / checkpoint read-set, repeat | 394.93 | 367.48 | 13,354.41 | 15,461.63 |

Values are microseconds, each the median of four run-level nearest-rank percentiles. Each run used 100 transactions per client. For the four-client disjoint checkpoint read-set case, the candidate p50 was 31% higher in the primary run and 7% lower in the repeat. The large and inconsistent p99 values are a reason to investigate scheduling and retries rather than infer a latency guarantee from this sample.

The workload uses the existing, cancellation-disabled APIs to read and update two checksummed counters in an atomic transaction. Each fresh fixture is verified after reopening. Both separately built binaries use the same benchmark source; the runner alternates their order and records all samples, retries and VM noise. The [primary report](../benchmarks/results/2026-11-04-transaction-cancellation-primary.json) and [repeat](../benchmarks/results/2026-11-04-transaction-cancellation-repeat.json) include all 16 combinations of shared/disjoint keys, one/four clients, and ordinary/checkpoint conservative/read-set modes. They record 128 successful runs each and no failures. All 256 pre-run quiet windows passed and no run recorded cgroup CPU throttling; host scheduling and I/O interference remain possible.

The baseline source was `85300ddb1c018bb1c681fe6a21740cb64f82edb6`. The candidate was [`f5948a6b22f320ca3c4e5c75b80861af4d06f2e3`](https://github.com/Eta06/EMDA-DB/tree/f5948a6b22f320ca3c4e5c75b80861af4d06f2e3), retained on the benchmark branch; its engine code matches the cancellation implementation subsequently integrated on main. Binaries had different recorded SHA-256 hashes and used isolated Cargo target directories and Rust 1.99.1. Runs used Linux 6.18.57 on an Intel Xeon Platinum 8573C VM, an overlay filesystem, a four-CPU cgroup quota, benchmark CPUs 0–3 and driver CPU 4.

This measures neither controlled transactions with cancellation enabled nor external network clients, cold starts, total memory, physical power loss, off-host recovery, or competitor systems. The existing transaction implementations, retry behavior and durability assumptions still apply. More repetitions on representative hardware and workloads are needed before drawing a performance conclusion.
