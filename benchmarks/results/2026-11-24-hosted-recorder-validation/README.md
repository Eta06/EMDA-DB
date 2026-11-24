# Hosted benchmark recorder validation

Two hosted runs at revision `2470201` produced 46 complete command
attempts and 24 JSON sample reports. The [main CI workflow](https://github.com/Eta06/EMDA-DB/actions/runs/45292962069)
recorded 44 benchmark smoke commands and 22 sample reports. The
[cross-engine workflow](https://github.com/Eta06/EMDA-DB/actions/runs/45292961990)
recorded two commands and two sample reports covering compound range
queries and process-restart checks.

The [archive](raw-artifacts.tar.gz) retains the uploaded files and both
workflow records. Every attempt metadata file matches the workflow's
revision, reports exit status zero and references a present original
command log. Sample reports parse as JSON. The benchmark settings,
durability configuration and verification described in each report govern
the use of its timings. These are smoke workloads. The release-tag
workflow did not run as part of this validation.

See [benchmark recording](../../../docs/BENCHMARKS.md#competitor-benchmark-records)
for the next attempt's expected files.
