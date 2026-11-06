# Interrupted filesystem calls — 6 November 2026

The [hosted campaign](https://github.com/Eta06/EMDA-DB/actions/runs/42167908971)
on clean revision `29bd317ff2c90c5514ad65c26b609928981f6367` used Linux
`strace` to return `EINTR` at selected successful-path filesystem calls. It
then checked the source, reference backup and checkpoint, visible copies and
both writable recovery orders. Sync calls included resolved ancestors;
data, read, open and namespace calls were selected from the disposable
fixture. Each group used 26 V2/V3 reference-fixture and operation controls;
the new-store initialization target used the filesystem's default format.

| Syscall group | Injected cases | Operation returned success | Operation returned error |
| --- | ---: | ---: | ---: |
| Synchronization | 818 | 818 | 0 |
| Data and metadata | 206 | 168 | 38 |
| Reads | 888 | 888 | 0 |
| Opens | 682 | 662 | 20 |
| Namespace operations | 148 | 56 | 92 |
| **Total** | **2,742** | **2,592** | **150** |

The success cases include calls retried by the runtime and cases where an
error occurred during later cleanup; success does not by itself prove a retry.
Every reported successful action had to be visible after recovery. A write
that returned an error retained its documented unknown outcome; the checks
verified prior acknowledged data and accepted only the specified recovered
states. Each recovery order was exercised in 1,371 cases.

The [archived reports](../benchmarks/results/2026-11-06-hosted-eintr-campaign.tar.gz)
retain the traces and fixture hashes from the GitHub ext4 runner. After
download, the independent
[`verify_one_shot_fs_results.py`](../scripts/verify_one_shot_fs_results.py)
checked the selected ordinals, errno and recovery orders in all five reports.
`strace` supplied a returned errno; it did not deliver an actual signal or
simulate a partial successful syscall, storage reordering, physical power
loss or off-host restore. Failure-only calls and arbitrary combinations of
interruptions remain outside this finite model.
