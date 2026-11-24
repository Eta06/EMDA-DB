# Combined writable checkpoint recovery scan budget

Revision `aacf60c5` adds independent combined scan admission to explicit
and managed writable checkpoint opening. The per-open budget charges
the authenticated encoded checkpoint once and the full logical log
scan. A complete pending tail adds a confirming pass over the entire
suffix after the checkpoint. If a log-only scan ceiling is also supplied,
both ceilings must fit.

For a checkpoint of size **I**, log length **L** and covered boundary
**P**, the minimum successful logical budget is **I + L** when the suffix
is already witnessed and **I + L + (L − P)** when the tail needs adoption.
The minimum is screened using file sizes before reading the checkpoint
header. The prefix in the header supplies the conditional charge for
the pending-tail pass. Admitted recovery authenticates the checkpoint
and rechecks the budget against the current log length before replay.
Metadata and framing probes have separate costs.

## What the regressions verify

- Explicit and managed opens reject one byte short and accept the exact
  budget, both with a pending complete batch and after it is witnessed.
- A zero combined budget rejects. A larger combined ceiling cannot
  bypass a smaller independently supplied log-only ceiling.
- Budget refusal can precede detection of corrupt checkpoint or log
  bytes. Once admitted, damaged checksummed bytes still fail verification.
- A managed automatic stage remains in place after budget rejection.
  Writer opening and selection synchronization are unreachable at
  that boundary. At the exact budget, recovery completes stage cleanup
  and adopts the pending tail.

The integration fixture uses an acknowledged suffix write followed by
an unacknowledged batch with a delete and a put. It compares the source
log, witness, anchor and selection around rejections and reopens. The
managed stage test separately checks a preserved owned payload and
successful cleanup. These are logical scan limits for a single open.

## Cross-platform verification

The native macOS Rust 1.85 library and integration suite passed 1,359
tests across 112 nonempty binaries. [Hosted macOS] passed both
toolchains. [Hosted Linux] passed Rust 1.85, stable, and the optimized
suite. The [filesystem EIO], [ENOSPC], and [cross-engine smoke]
workflows passed.

The [raw archive] includes 51 benchmark attempts, comprising 45 CI
smoke cases, five cross-engine cases, and the hosted Linux million-key
recovery workload. The Linux workload verified all 53 phases, with a
48 MiB kernel address-space ceiling in 25 phases. The archive
preserves individual results, native and hosted validation logs,
verification scripts and a SHA-256 manifest.

[hosted Linux]: https://github.com/Eta06/EMDA-DB/actions/runs/45432558144
[hosted macOS]: https://github.com/Eta06/EMDA-DB/actions/runs/45432558107
[filesystem EIO]: https://github.com/Eta06/EMDA-DB/actions/runs/45432558388
[ENOSPC]: https://github.com/Eta06/EMDA-DB/actions/runs/45432558897
[cross-engine smoke]: https://github.com/Eta06/EMDA-DB/actions/runs/45432558211
[raw archive]: raw-artifacts.tar.gz
