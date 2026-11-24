# Hosted benchmark smoke runs after checkpoint landmark packing

Revision `c6386ee` recorded 44 command logs and 22 sample reports in
[Linux CI](https://github.com/Eta06/EMDA-DB/actions/runs/45306994620).
The [cross-engine workflow](https://github.com/Eta06/EMDA-DB/actions/runs/45306994516)
recorded three more attempts and sample reports for compound-range reads,
large-value recovery, and concurrent document updates.

All 47 commands exited successfully. The [raw archive](raw-artifacts.tar.gz)
preserves all command logs, attempt metadata, sample reports, and workflow
metadata. The 121 source files were verified against the included SHA-256
manifest after extraction. These smoke cases exercise benchmark integrations
and use small workloads.
