# V3 fixture correction cross-engine smoke runs

The [raw archive](raw-artifacts.tar.gz) preserves all three cross-engine
smoke cases at `5613fe9`. Each case passed. The workloads cover compound
range queries, document contention, and process crashes.
[Cross-engine CI](https://github.com/Eta06/EMDA-DB/actions/runs/45338895803).

The command, output, exit status, and raw samples are retained for each
case. The embedded file hashes were verified after extraction. These
short cases exercise the harness and supply preliminary measurements.
