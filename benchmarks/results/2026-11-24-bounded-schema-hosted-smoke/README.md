# Hosted smoke benchmarks for bounded document schemas

Revision `bfed77f` recorded 44 commands and 22 JSON sample reports in
[Linux CI](https://github.com/Eta06/EMDA-DB/actions/runs/45297950206).
The [cross-engine workflow](https://github.com/Eta06/EMDA-DB/actions/runs/45297950046)
recorded two more commands and two reports for compound range queries and
large values with process termination. All 46 commands exited successfully.

The [raw artifacts](raw-artifacts.tar.gz) preserve commands, runner metadata,
output logs, individual samples and workflow metadata. The bundled
`SHA256SUMS` verifies the 118 archived files. These small smoke cases exercise
the benchmark harness and competitor adapters. Use larger repeated workloads
to estimate production latency, throughput and resource usage.
