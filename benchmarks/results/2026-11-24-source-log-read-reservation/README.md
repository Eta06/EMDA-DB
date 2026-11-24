# Source-log read reservation evidence

Four Linux `strace` baselines at `b2147fb` recorded every source-log
`read` and `pread64` request for full-log and trusted-checkpoint rebuilds
with complete pending tails on V2 and V3. Each sum matched the admitted
reservation. The [raw evidence](rebuild-witness-faults.jsonl) also contains
all 38 witness and selection fault cases from the same job.
[Filesystem EIO CI](https://github.com/Eta06/EMDA-DB/actions/runs/45342929584).

The [raw benchmark archive](raw-cross-engine.tar.gz) contains three
successful hosted smoke cases at the same revision. They cover compound
range queries, document contention and process crashes. All embedded
hashes were verified after extraction.
[Cross-engine CI](https://github.com/Eta06/EMDA-DB/actions/runs/45342929709).
