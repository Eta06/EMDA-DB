# Combined scan admission for writable retained recovery

Revision `0866d87` adds `open_with_recovery_scan_limit` to standalone,
managed and authority-backed retained stores. It charges one encoded
base scan plus one complete suffix replay. When the local witness
trails the physical suffix, it reserves a second suffix scan before
either payload is read. The existing suffix-only entry points retain
their scope; the new limit is supplied per open and leaves persisted
generation and catalog limits in effect.

In the V2 probe the encoded base is 187 bytes and the suffix is 59 bytes.
An open with a pending tail requires 305 bytes, while reopening after
adoption requires 246. One byte less rejects each open before witness
adoption. The standalone test also verifies that an admitted open
rejects a damaged base. The managed case preserves the selection and
catalog settings. The authority-backed case preserves the exact pending
intent on rejection and finalizes it after an admitted reopen.

The cap measures logical payload scans. Framing and metadata reads,
the base EOF probe, physical I/O retries and reads after open have
separate costs.

Native macOS passed all 1,329 Rust tests across 112 nonempty binaries.
Hosted [macOS] and [Linux] passed on both toolchains, including optimized
Linux. The [filesystem fault] job passed 16 scan-boundary checks across
V2 and V3 source fixtures and both budget modes. Rejection read zero
base and suffix payload bytes. The exact combined limits read the 187-byte
base and the expected 118 or 59 suffix bytes. Log and selection contents
matched across attempts. The [ENOSPC] workflow also passed.

The native full Python discovery attempt used system Python 3.9 and
encountered missing newer modules and Linux platform APIs. The focused
workflow checks passed with Homebrew Python, and the full script suite
passed in Linux CI. Both native Python logs are included.

The [raw archive](raw-artifacts.tar.gz) includes the traces, all 45
standard smoke attempts, five [cross-engine smoke] attempts and all 51
phases of the four-writer recovery workload, including 24 run at a 48 MiB
address-space cap. Physical power-loss testing and independent off-host
restore remain deferred.

[Linux]: https://github.com/Eta06/EMDA-DB/actions/runs/45383673286
[macOS]: https://github.com/Eta06/EMDA-DB/actions/runs/45383673314
[filesystem fault]: https://github.com/Eta06/EMDA-DB/actions/runs/45383673093
[ENOSPC]: https://github.com/Eta06/EMDA-DB/actions/runs/45383673257
[cross-engine smoke]: https://github.com/Eta06/EMDA-DB/actions/runs/45383673255
