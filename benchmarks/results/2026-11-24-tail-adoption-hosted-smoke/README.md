# Hosted smoke benchmarks after checkpoint tail adoption changes

Revision `6189979` recorded 44 command logs and 22 sample reports in
[Linux CI](https://github.com/Eta06/EMDA-DB/actions/runs/45305908384).
The [cross-engine workflow](https://github.com/Eta06/EMDA-DB/actions/runs/45305908396)
recorded three more logs and reports for compound-range reads,
large-value recovery, and concurrent document updates.

All 47 commands exited successfully. The [raw archive](raw-artifacts.tar.gz)
preserves their original output, sample reports, workflow metadata and
checksums. These smoke workloads check the benchmark integrations.
