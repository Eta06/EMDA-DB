# Hosted smoke benchmarks after managed rebuild outcome reporting

[Linux CI](https://github.com/Eta06/EMDA-DB/actions/runs/45317503157) at
`7f54cce` recorded 44 attempts and 22 sample reports.
The [cross-engine workflow](https://github.com/Eta06/EMDA-DB/actions/runs/45317503212)
recorded three more attempts and reports for compound-range queries,
large-value recovery, and concurrent document updates.

All 47 commands exited successfully. The [raw archive](raw-artifacts.tar.gz)
contains each command log, attempt record, sample report, workflow metadata,
and a SHA-256 manifest. All 121 files matched the manifest after extraction.
These are smoke workloads for verifying the benchmark integrations.
