# Linux smoke evidence after source-log read reservation

The [raw archive](raw-artifacts.tar.gz) contains all 45 benchmark attempts at
`b2147fb`, including SQLite controls, backup and restore drills, and the
checkpoint and retained recovery probes. Both Linux test jobs passed. All
45 commands and their wrappers returned zero. The archive retains the
commands, outputs, structured samples and hosted job metadata. All 114 file
hashes verified after extraction.

The same revision passed native macOS correctness. The
[source-log read reservation report](../2026-11-24-source-log-read-reservation/README.md)
records the Linux fault traces and the separate three-case cross-engine
suite. The smoke workloads exercise correctness and record timings on a
shared runner. Dedicated comparisons use separately recorded environments
and repeated workloads.

[Linux CI](https://github.com/Eta06/EMDA-DB/actions/runs/45342929537) ·
[macOS correctness](https://github.com/Eta06/EMDA-DB/actions/runs/45342929576)
