# Hosted benchmark smoke runs after packing rollover verification probes

Revision `378cde7` recorded 44 command logs and 22 sample reports in
[Linux CI](https://github.com/Eta06/EMDA-DB/actions/runs/45309599714).
The [cross-engine workflow](https://github.com/Eta06/EMDA-DB/actions/runs/45309599677)
recorded three additional attempts and sample reports for compound-range
queries, large-value recovery, and concurrent document updates.

All 47 commands exited successfully. The [raw archive](raw-artifacts.tar.gz)
contains the original command output, attempt metadata, sample reports,
workflow metadata and SHA-256 manifest. All 121 source files matched the
manifest after extraction. The small workloads exercise the integrations.
