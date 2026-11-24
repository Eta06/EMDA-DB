# Hosted smoke benchmarks for the retired snapshot log limit

Revision `98dee48` recorded 44 commands and 22 JSON sample reports in
[Linux CI](https://github.com/Eta06/EMDA-DB/actions/runs/45300070659).
The [cross-engine workflow](https://github.com/Eta06/EMDA-DB/actions/runs/45300070588)
recorded two commands and two reports for compound range queries and
large values with process termination. All 46 commands exited successfully.

The [raw artifacts](raw-artifacts.tar.gz) retain the commands, runner
metadata, output logs, individual samples and workflow metadata.
The bundled `SHA256SUMS` verifies the 118 data files. These smoke
runs check integration on small fixtures.
