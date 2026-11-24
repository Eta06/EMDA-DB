# Retained writable recovery under a Linux memory ceiling

The [raw archive](raw-artifacts.tar.gz) preserves the hosted Linux x86-64
run at `38e8988`, including command, exit status, environment, phase logs,
measurements and hosted job metadata. Flush and verification used the
normal retained catalog. All 44 archived files verified against their
hashes after extraction.

The run started with 262,144 keys and executed four epochs of 32,768 ordered
mutations per backend, using the wide-suffix policy. All three engines
passed every fresh-process point check and full ordered verification scan.
Retained EMDA rotated once during mutation. All 12 retained mutate, reopen
and verify workers completed under a 48 MiB hard and soft `RLIMIT_AS`, with
a maximum observed RSS of 7,499,776 bytes (7.15 MiB). The archive preserves
all 38 phases, including uncapped ordinary EMDA and SQLite WAL/FULL controls.

The kernel ceiling applies individually to each timed retained worker and
its execution environment. Setup, initial cutover and control workers ran
uncapped. The result supplies measured memory evidence for this writable
recovery workload. Filesystem cache, simultaneous tenants, aggregate
project usage and other workloads have separate resource costs.

The archive also contains a native macOS result from the same Rust source
tree. All 182 selected retained library tests passed on Rust 1.85.0. The
enforced ceiling was exercised on Linux.

[Hosted CI](https://github.com/Eta06/EMDA-DB/actions/runs/45344487724) ·
[Reproduction command](../../../examples/authorized_recovery_bench.md)
