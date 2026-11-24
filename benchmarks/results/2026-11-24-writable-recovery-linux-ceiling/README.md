# Writable checkpoint and retained recovery under a Linux memory ceiling

The [raw archive](raw-artifacts.tar.gz) records the hosted Linux run at
`0057f3d`. All 57 file hashes verified after extraction. It preserves the
command, exit status, binary hash, phase logs, measurements, hosted job
metadata and the coverage check.

Four backends started with 262,144 keys and ran four epochs of 32,768
ordered mutations. Every fresh-process point check and complete ordered
scan passed. The checkpoint boundary advanced in every epoch and the
retained catalog rotated once. Both writable recovery paths completed all
24 mutation, reopen and verify phases under a hard and soft 48 MiB Linux
`RLIMIT_AS`. Checkpoint peak process RSS was 6,811,648 bytes and retained
peak process RSS was 7,761,920 bytes. All 51 phases are recorded, including
ordinary EMDA-DB and SQLite WAL/FULL controls.

The ceiling applies to each selected worker process. Fixture preparation,
initial cutover and control workers ran uncapped. Checkpoint reopen still
verifies the covered log, which grows across epochs. Filesystem cache and
aggregate project consumption are outside the process ceiling.

[Hosted CI](https://github.com/Eta06/EMDA-DB/actions/runs/45347413883) ·
[Benchmark guide](../../../examples/authorized_recovery_bench.md)
