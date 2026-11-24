# Linux concurrent writable recovery under a memory ceiling

At `dd61154`, four clients performed 32,768 operations in 256 batches
per backend and epoch, starting with 262,144 keys. All four backends
matched the complete workload model after each fresh-process reopen
over four epochs. All 51 phases passed, including 24 checkpoint and
retained phases under a 48 MiB process address-space ceiling. The
workload advanced the checkpoint in all four epochs and completed
one authorized retained rotation.

| Backend | Median mutation phase, ms | Peak RSS across mutate, reopen and verify, MiB |
| --- | ---: | ---: |
| Ordinary | 55.5 | 35.91 |
| Checkpoint | 344.5 | 7.29 |
| Authorized retained | 323.3 | 7.38 |
| SQLite WAL with synchronous FULL | 68.3 | 7.28 |

Mutation times include worker startup and any rollover or rotation.
Authorized retained clients serialize writes through one authority
owner. SQLite uses one connection per client. Only the checkpoint
and retained phases run with the address-space cap. These samples
use one hosted Linux x86-64 machine and clean restarts. The cap
does not establish a project-wide memory limit.

The [raw archive](raw-artifacts.tar.gz) preserves every phase and
the hosted compound range, concurrent document and process-crash
comparison cases from the same revision. It includes the disposable
ENOSPC test outcome. The Linux job report was captured once the
memory-ceiling job completed.

[Linux CI](https://github.com/Eta06/EMDA-DB/actions/runs/45352395209) ·
[Cross-engine smoke](https://github.com/Eta06/EMDA-DB/actions/runs/45352395114) ·
[Disposable ENOSPC](https://github.com/Eta06/EMDA-DB/actions/runs/45352395064) ·
[macOS smoke archive](../2026-11-24-four-writer-recovery-macos/README.md)
