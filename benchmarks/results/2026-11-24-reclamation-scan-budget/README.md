# Retained reclamation scan budget

Revision `2766ee9` adds an optional per-call limit to explicit retirement
on managed and authority-backed retained stores. Each complete verification
of the selected replacement charges its encoded base and suffix. Separate
verifications for resumed cleanup and a new retirement use the same budget.
The charge must fit before that verification begins. The authority-backed
path admits the new scan before submitting the fence and still verifies
the replacement before moving or deleting the inactive generation.

The macOS file-backed authority probe used a 206-byte workload. A 205-byte
budget returned `InvalidInput` with the fence unattempted and removal
unstarted. The exact budget retired the inactive slot and advanced the
authority once, preserving the active log, base and witness.

A managed test creates an owned empty retirement stage alongside the
inactive slot, the state possible after interruption before moving the
slot. One byte below the first verification leaves both in place. One
verification's exact budget resumes the old stage and reports the
removal attempt when the new retirement exceeds the remaining budget.
A clean retry retires the inactive slot and preserves the acknowledged
suffix. Authority-backed tests also check a resumed retirement after a
failed move sync and preserve the fence across retries.

The limit measures verification payload reads. Metadata and framing
checks, the EOF probe, physical I/O retries and automatic maintenance have
separate costs. Physical power-loss testing and independent off-host
restore remain deferred.

Native macOS Rust 1.85.0 passed 1,330 tests across 112 nonempty binaries.
Formatting, warnings-denied Clippy and rustdoc passed. Hosted [macOS]
and [Linux] passed on both toolchains, including optimized Linux. The
[filesystem fault] and [ENOSPC] workflows passed. The Linux reclamation
trace separated the reads needed to open the fixture from the additional
verification reads made by the retirement call. The 205-byte cap
caused zero additional payload reads. The 206-byte cap read the
168-byte encoded base and 38-byte suffix, then retired the inactive slot.

The [raw archive](raw-artifacts.tar.gz) includes the authority and
filesystem traces, all 45 standard benchmark smoke attempts, all five
[cross-engine smoke] attempts, and the four-writer checkpoint and retained
recovery workload. All 51 workload phases passed, including 24 under a
48 MiB address-space limit. The archive includes logs, revisions and
a checksum manifest.

[Linux]: https://github.com/Eta06/EMDA-DB/actions/runs/45385831058
[macOS]: https://github.com/Eta06/EMDA-DB/actions/runs/45385831216
[filesystem fault]: https://github.com/Eta06/EMDA-DB/actions/runs/45385831050
[ENOSPC]: https://github.com/Eta06/EMDA-DB/actions/runs/45385830967
[cross-engine smoke]: https://github.com/Eta06/EMDA-DB/actions/runs/45385830952
