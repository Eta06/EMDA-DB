# Retained base hashing experiments

Two independent changes were measured against `f425517`. One hashed
the retained read buffer at refill time. The other hashed bytes as
the buffered successor writer flushed them. Both candidates passed
the focused integrity and document recovery tests. Neither candidate
improved rotating write CPU on Linux, so production retains the
original implementation.

Each trial paired six epochs at 524,288 initial keys. Both binaries
encountered four rotation epochs. Each epoch verified the first read
and independently checked every live key and value. All 48 full-state
comparisons passed. macOS used APFS; Linux ARM64 used tmpfs. Rust
1.85 release builds used distinct binary hashes and alternated order.

| Experiment | Platform | Median rotating write CPU before / after | Median verified scan CPU before / after |
| --- | --- | ---: | ---: |
| Hash at read refill | macOS | 210.65 / 220.41 ms | 87.34 / 94.26 ms |
| Hash at read refill | Linux | 119.53 / 129.51 ms | 68.75 / 76.96 ms |
| Hash buffered successor writes | macOS | 233.36 / 223.03 ms | 89.06 / 87.86 ms |
| Hash buffered successor writes | Linux | 120.13 / 126.35 ms | 66.02 / 65.55 ms |

The rotating-write median covers four epochs per binary. Scan medians
cover all six. The benchmarks measure overall phase CPU, including
the other verification and authority work. These are small, warm
campaigns with separately populated fixtures. They provide no basis
for attributing the CPU differences to a particular instruction or
predicting them on other hardware.

The [raw archive](raw-results.tar.gz) contains the two source diffs
against `f425517`, environments, runner and measurements. To repeat a
trial, build the unchanged base and the chosen diff in different
Cargo target directories, each with
`cargo +1.85.0 build --locked --release --example authorized_recovery_bench`.
Run the archived `paired.py` with the baseline binary, candidate binary,
`examples/authorized_recovery_bench.py`, a fresh result directory, and
`524288` as arguments, setting `EMDA_BENCH_SUFFIX_WORK=240000000`.
