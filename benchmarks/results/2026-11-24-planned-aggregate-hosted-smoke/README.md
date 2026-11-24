# Hosted smoke benchmarks after indexed grouped statistics

[Linux CI](https://github.com/Eta06/EMDA-DB/actions/runs/45320616918)
at `20124c8` recorded 45 successful attempts and 23 reports, including
the first indexed aggregate statistics comparison with SQLite.
The [cross-engine workflow](https://github.com/Eta06/EMDA-DB/actions/runs/45320616958)
recorded three successful attempts covering compound ranges, large-value
recovery, and concurrent document updates.

The [raw archive](raw-artifacts.tar.gz) contains each command log, attempt
record, sample report, workflow metadata and a SHA-256 manifest. All 124
files matched the manifest after extraction. These workloads exercise
the benchmark integrations.
