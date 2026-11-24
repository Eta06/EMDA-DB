# Hosted benchmark smoke runs for online snapshot deadlines

The [Linux CI run](https://github.com/Eta06/EMDA-DB/actions/runs/45296792942)
on `c90206d` captured 44 smoke commands and 22 JSON sample reports. The
cross-engine workflow captured two commands and two reports for each
of [`b96c162`](https://github.com/Eta06/EMDA-DB/actions/runs/45295862063),
[`7315c55`](https://github.com/Eta06/EMDA-DB/actions/runs/45296444320)
and [`c90206d`](https://github.com/Eta06/EMDA-DB/actions/runs/45296793370).
All 50 recorded commands exited successfully. The main workflow was
superseded during optimized validation on `b96c162` and failed at lint
on `7315c55`, before its benchmark smoke step on either revision.

The [raw artifacts](raw-artifacts.tar.gz) retain the command logs,
individual reports and workflow metadata. Each command record matches
its workflow's revision and points to a present original output log.
These are integration smoke workloads.
