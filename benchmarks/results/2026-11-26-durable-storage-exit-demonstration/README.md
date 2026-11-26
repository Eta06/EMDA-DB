# Durable-storage exit demonstration

One workload, `examples/exit_demonstration.rs`, exercises the four exit
criteria of the durable storage priority on revision `b745ace5`:
independent concurrent clients, transactional atomicity, recovery of
acknowledged writes under the documented fault model, and memory within a
configured limit.

## Workload

A managed checkpoint store holds 16 accounts of 1,000 units each. It has
automatic checkpoints, a 16 KiB suffix limit that forces frequent
rollovers, and a 64 MiB persistent disk budget.

Each round starts a child process with four client threads. Each client
repeats one read-set transaction:

1. read two accounts;
2. move up to 10 units between them;
3. advance its own counter in the same transaction.

It prints an acknowledgement only after the commit returns. The parent
kills the child with `SIGKILL` after a varying number of acknowledgements,
then reopens the store. The reopened store must show:

- the account total unchanged, so no transfer was partly applied;
- every acknowledged counter value present;
- at most one unacknowledged durable commit per client, the one that may
  have been in flight;
- the disk budget with nothing pending or reserved and its charge within
  the limit;
- automatic checkpoints still enabled, with at least one automatic slot
  published.

## Results

| Host | Rounds × clients | Acknowledged | Recovered | Unacknowledged durable | Slots | Peak RSS |
| --- | --- | ---: | --- | ---: | ---: | ---: |
| macOS APFS, Apple M6, local | 12 × 4 | 1,198 | all, 12 reopens | 12 | 2 | 3.05 MB |
| Hosted Linux ext4, 64 MiB address-space limit | 12 × 4 | 1,198 | all, 12 reopens | 4 | 2 | 3.72 MB |

On Linux, the memory-ceiling job of [CI] ran the demonstration under
`prlimit --as` with 64 MiB. Every client process inherits that limit. The
job checks the exit status, that the limit was applied (`rlimit_as_kib=65536`),
the recovery counts, the budget, the published slots and peak RSS. Peak RSS
is the demonstration parent's, from `/usr/bin/time`. Clients were bounded by
the inherited address-space limit, not measured separately.

The [CI] run failed in its other two jobs on a Clippy lint in an unrelated
new test, fixed in the following commit. The memory-ceiling job that
carries this demonstration passed.

## Scope

These are process kills on a running OS, not power loss. The physical
power-cut drill is deferred to disposable hardware. Clients are threads in
one process per round, sharing one embedded store; there is no network
service. The address-space limit bounds each process, not the machine or a
project.

[CI]: https://github.com/Eta06/EMDA-DB/actions/runs/45844834376
[raw archive]: raw-artifacts.tar.gz

The [raw archive] holds the macOS run output and time report, the hosted
CI record and log excerpt, the workload source and `SHA256SUMS`.
