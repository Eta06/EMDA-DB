# Hosted smoke benchmarks after ordinary batch encoding changes

[Linux CI](https://github.com/Eta06/EMDA-DB/actions/runs/45316359389) at
`7b5752b` recorded 44 attempts and 22 sample reports.
The [cross-engine workflow](https://github.com/Eta06/EMDA-DB/actions/runs/45316359275)
recorded three more attempts and reports for compound-range queries,
large-value recovery, and concurrent document updates.

All 47 commands exited successfully. The [raw archive](raw-artifacts.tar.gz)
contains each command log, attempt record, sample report, workflow metadata,
and a SHA-256 manifest. All 121 files matched the manifest after extraction.
These are smoke workloads for verifying the benchmark integrations.
