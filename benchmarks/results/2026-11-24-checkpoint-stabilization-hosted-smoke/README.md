# Hosted smoke benchmarks after checkpoint metadata stabilization

[Linux CI](https://github.com/Eta06/EMDA-DB/actions/runs/45319596040)
at `80bf5b6` recorded 44 successful attempts and 22 sample reports.
The [cross-engine workflow](https://github.com/Eta06/EMDA-DB/actions/runs/45319595994)
recorded three successful attempts covering compound ranges, large-value
recovery, and concurrent document updates.

The [raw archive](raw-artifacts.tar.gz) contains each command log, attempt
record, sample report, workflow metadata and a SHA-256 manifest. All 121
files matched the manifest after extraction. These workloads exercise
the benchmark integrations.
