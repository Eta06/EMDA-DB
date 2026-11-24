# Hosted smoke benchmarks after guarded negated ranges

[Linux CI](https://github.com/Eta06/EMDA-DB/actions/runs/45310672921) on
revision `1a23c7a` recorded 44 attempts and 22 sample reports.
The [cross-engine workflow](https://github.com/Eta06/EMDA-DB/actions/runs/45310673018)
recorded three additional attempts and sample reports for compound-range
queries, large-value recovery, and concurrent document updates.

All 47 commands returned successfully. The [raw archive](raw-artifacts.tar.gz)
preserves all command output, attempt metadata, reports, workflow metadata,
and the SHA-256 manifest. All 121 source files matched their checksums after
extraction. These small workloads exercise the benchmark integrations.
