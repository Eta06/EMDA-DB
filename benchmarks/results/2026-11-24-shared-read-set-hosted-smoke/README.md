# Hosted smoke benchmarks after shared read-set validation

[Linux CI](https://github.com/Eta06/EMDA-DB/actions/runs/45315379094) on
revision `fecc81b` recorded 44 attempts and 22 sample reports.
The [cross-engine workflow](https://github.com/Eta06/EMDA-DB/actions/runs/45315379092)
recorded three more attempts and reports for compound-range queries,
large-value recovery, and concurrent document updates.

All 47 commands exited successfully. The [raw archive](raw-artifacts.tar.gz)
preserves command logs, attempt metadata, sample reports, workflow metadata
and a SHA-256 manifest. All 121 source files matched their checksums after
extraction. These cases exercise the benchmark integrations.
