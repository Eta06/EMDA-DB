# Hosted smoke benchmarks for document contention recording

Revision `9e1067c` recorded 44 commands and 22 individual sample reports in
[Linux CI](https://github.com/Eta06/EMDA-DB/actions/runs/45303724900).
The [cross-engine workflow](https://github.com/Eta06/EMDA-DB/actions/runs/45303724902)
recorded three more commands and three reports for compound range reads,
large-value process recovery, and concurrent document updates. The
contention report records all 20 embedded and server runs as verified.

All 47 commands exited successfully. The [raw archive](raw-artifacts.tar.gz)
contains the original output logs, sampled results, workflow metadata and
checksums. These are integration smoke workloads.
