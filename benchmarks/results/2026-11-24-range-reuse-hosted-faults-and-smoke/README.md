# Hosted verification for ordinary read-set range reuse

The [raw archive](raw-artifacts.tar.gz) preserves 49 successful benchmark
attempts at `008e9e5`: 45 standard Linux smoke cases, the expanded writable
memory gate, and three cross-engine workloads. It also retains returned-error
and disposable tmpfs ENOSPC results and hosted job metadata. All 305 file
hashes verified after extraction.

Rust 1.85.0 and stable passed on macOS and Linux. The Linux gate again
completed 24 capped phases across checkpoint and retained recovery.
The syscall report retains 38 returned-error cases and four rebuild
read traces across V2 and V3. The ENOSPC assertions and cleanup passed.

[Linux CI](https://github.com/Eta06/EMDA-DB/actions/runs/45348772642) ·
[macOS correctness](https://github.com/Eta06/EMDA-DB/actions/runs/45348772793) ·
[Cross-engine smoke](https://github.com/Eta06/EMDA-DB/actions/runs/45348772595) ·
[Filesystem faults](https://github.com/Eta06/EMDA-DB/actions/runs/45348772701) ·
[ENOSPC](https://github.com/Eta06/EMDA-DB/actions/runs/45348772659)
