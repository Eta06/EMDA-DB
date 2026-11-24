# Bounded retained startup and retirement

Revision `6ec385d7` adds one logical read ceiling for retained recovery
and startup retirement. Recovery charges the encoded base and suffix
and a second suffix pass when the local witness needs adoption.
Admission reserves a full current-generation verification for an
inactive slot and a separate verification when retained retirement
markers exist. The reservation precedes replay, any witness update,
authority resolution and cleanup. Once admitted, recovery can finish
before a later cleanup error. Subsequent mutations use a separate
rotation and retirement budget.

The boundary regressions exercise managed tail adoption, authorized
pending-intent resolution, and restart after a failed retirement sync.
A short limit leaves the witness or authority and retained files
untouched. The exact budget admits recovery and verified retirement.
The managed test also exercises independent maintenance refusal on
the first later write.

## Linux recovery and comparison

All 53 phases passed. The 25 capped phases reported the expected 48 MiB
kernel soft and hard address-space limits. All 12 retained epoch phases
reported the 432 MiB startup scan limit and 1,120 MiB per-mutation
maintenance scan limit. One retained rotation and at least one
checkpoint rollover completed. The retained lifecycle peaked at
11,378,688 bytes of process RSS and verified all 1,045,597 final live
entries in each of the current and restored catalogs.

| Engine | Mutate | Reopen to first verified read | Full verification |
| --- | ---: | ---: | ---: |
| EMDA ordinary | 66.12 | 415.90 | 362.41 |
| EMDA checkpoint | 1,078.90 | 159.30 | 376.87 |
| EMDA retained | 262.01 | 187.81 | 269.66 |
| SQLite WAL/FULL | 69.86 | 0.85 | 275.69 |

Values are median milliseconds across four epochs with 1,048,576
initial keys and 32,768 operations per epoch, driven by four writers
on one hosted Linux runner. Reopen performs engine-specific
verification. Ordinary and SQLite ran without the harness-imposed
address-space ceiling. The workflow used the runner's normal caches.

## Native macOS smoke

An ARM64 Rust 1.85 release build ran one 4,096-key epoch with two writers
and 4,096 operations per engine. All four engines verified 3,724 ordered
live key/value entries. Five retained rotations completed under the
99 MiB startup scan limit and 262 MiB per-mutation maintenance scan
limit. This single warm-cache run shared the host with correctness
testing. Times below describe the smoke attempt.

| Engine | Mutate | Reopen to first verified read | Full verification |
| --- | ---: | ---: | ---: |
| EMDA ordinary | 786.23 | 41.31 | 5.75 |
| EMDA checkpoint | 778.90 | 74.18 | 5.99 |
| EMDA retained | 7,249.79 | 162.44 | 9.81 |
| SQLite WAL/FULL | 1,437.12 | 23.43 | 5.91 |

An initial build against the shared release target resolved an older
benchmark dependency and failed. The timed run used a fresh isolated
target, its binary hash is recorded, and both build logs are included.

## Verification evidence

Native macOS Rust 1.85 passed 1,354 tests across 112 nonempty binaries.
[Hosted macOS] passed both toolchains. [Hosted Linux] passed Rust 1.85
and stable, including the optimized suite. The [filesystem EIO],
[ENOSPC] and [cross-engine smoke] workflows passed.

The [raw archive] preserves 52 benchmark attempt records and their
results. It contains 45 standard smoke cases, five cross-engine cases,
the Linux recovery run, and the native macOS smoke. It includes the
workflow logs, the independent phase assertions, and a SHA-256 manifest.

[hosted Linux]: https://github.com/Eta06/EMDA-DB/actions/runs/45417451328
[hosted macOS]: https://github.com/Eta06/EMDA-DB/actions/runs/45417451682
[filesystem EIO]: https://github.com/Eta06/EMDA-DB/actions/runs/45417451454
[ENOSPC]: https://github.com/Eta06/EMDA-DB/actions/runs/45417451377
[cross-engine smoke]: https://github.com/Eta06/EMDA-DB/actions/runs/45417451861
[raw archive]: raw-artifacts.tar.gz
