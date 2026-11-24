# Ordinary recovery cross-engine smoke runs

The [raw archive](raw-artifacts.tar.gz) preserves all three successful
cross-engine smoke cases at `a06b6db1`. The cases exercise compound range
queries, document read/write contention, and process-crash workloads.
[Cross-engine CI](https://github.com/Eta06/EMDA-DB/actions/runs/45337423013).

The archive includes each command, exit status, output and raw samples.
The embedded SHA-256 manifest was verified after extraction. These short
cases check that the harness runs and provide preliminary measurements.
