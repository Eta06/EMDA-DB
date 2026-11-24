# Writable retained recovery scan admission

Source `a3325fd` charges one logical suffix scan on a fully witnessed
generation and reserves a second scan when recovery would adopt a
pending tail. The limit applies to standalone, managed and
authority-backed writable retained opens. The catalog variants can also
enforce a per-open storage ceiling. Admitted opens retain the existing
base verification, witness synchronization and authority checks.

The Linux probe used a managed generation with a 59-byte suffix in
each of two fixtures imported from V2 and V3 source logs. The traces
count the requests into the retained suffix, excluding the generation
header and other metadata.

| Witness state | Byte cap | Outcome | Suffix reads |
| --- | ---: | --- | --- |
| Pending tail | 117 | Rejected before suffix or base reads | 0 |
| Pending tail | 118 | Recovered and adopted the tail | 59 + 59 |
| Fully witnessed | 58 | Rejected before suffix or base reads | 0 |
| Fully witnessed | 59 | Opened | 59 |

The probe checked identical log and selection bytes through all four
calls, exact witness preservation on rejection, and all expected
values. The macOS retained suite passed 124 tests, including damaged
suffix validation when admitted, simultaneous catalog quota checks
and preservation of a pending authority intent after a rejected open.
An authority intent with an already advanced local witness requires
one suffix scan before the existing synchronization and final CAS.

The native macOS Rust 1.85.0 run passed 1,323 tests across 112 nonempty
binaries. The initial focused compilation log records a test-only
constant-name error that was corrected before publication. Hosted
stable and Rust 1.85.0 suites passed on [macOS](https://github.com/Eta06/EMDA-DB/actions/runs/45365392833)
and [Linux](https://github.com/Eta06/EMDA-DB/actions/runs/45365392773).
Linux passed the optimized build and all 45 standard benchmark smoke
attempts. Its four-writer memory gate passed all 51 phases, including
24 checkpoint and retained phases capped at 48 MiB of process address space.

The [raw archive](raw-artifacts.tar.gz) preserves those attempts,
three [cross-engine smoke attempts](https://github.com/Eta06/EMDA-DB/actions/runs/45365392749),
the [Linux fault and syscall evidence](https://github.com/Eta06/EMDA-DB/actions/runs/45365392558),
and the [ENOSPC result](https://github.com/Eta06/EMDA-DB/actions/runs/45365392737).
