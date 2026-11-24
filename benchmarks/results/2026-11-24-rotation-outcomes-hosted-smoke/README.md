# Hosted smoke benchmarks after retained rotation outcome changes

[Linux CI](https://github.com/Eta06/EMDA-DB/actions/runs/45311763574) on
revision `92c4631` recorded 44 attempts and 22 sample reports.
The [cross-engine workflow](https://github.com/Eta06/EMDA-DB/actions/runs/45311763488)
recorded three additional attempts and reports for compound-range reads,
large-value recovery, and concurrent document updates.

All 47 commands returned successfully. The [raw archive](raw-artifacts.tar.gz)
preserves their output, attempt metadata, sample reports, workflow metadata,
and SHA-256 manifest. All 121 source files matched their checksums after
extraction. These small workloads exercise the benchmark integrations.
