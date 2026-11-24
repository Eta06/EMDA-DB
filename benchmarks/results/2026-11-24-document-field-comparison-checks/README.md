# Bounded document field comparisons

At `34e70bf`, 55 focused macOS tests passed with Rust 1.85.0,
covering query and aggregate agreement, staged reads, partial-index
presence inference and reopening on ordinary and checkpoint stores.
Formatting and warnings-denied Clippy passed. Hosted Linux returned-error
and ENOSPC checks passed alongside three compatibility smoke workloads.

The [raw archive](raw-artifacts.tar.gz) contains local test and lint
logs, the hosted fault results and all three comparison attempts.
It retains cancellation reports for the standard platform jobs on
this intermediate revision. The superseding scan-budget revision
includes the comparison feature.

[Cross-engine smoke](https://github.com/Eta06/EMDA-DB/actions/runs/45356583301) ·
[Filesystem EIO](https://github.com/Eta06/EMDA-DB/actions/runs/45356583167) ·
[Superseding Linux CI](https://github.com/Eta06/EMDA-DB/actions/runs/45356970859)
