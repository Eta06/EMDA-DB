# Read admission counter comparison

This macOS ARM64 APFS benchmark checks the steady-state cost of the
shared read admission counter. The baseline is `2470201`. The candidate
was built from that revision with the included production diff, before
adding the final guard for changing online admission scope with an
owned snapshot. Both used Rust 1.85.0 release builds and locked
dependencies. SQLite 3.49.1 WAL/FULL was built into the baseline
binary and timed as a competitor in the same rotation.

The workload uses 16 keys with 64-byte values and 40,000 reads per
client. A round alternates process order over both EMDA revisions
with admission disabled and enabled, and SQLite. The enabled mode
admits up to 65,535 direct calls. Ten rounds at one and four clients
produced 100 recorded attempts and verified reopens. The driver uses
fresh fixtures and short warmups before timing.

| Clients | Mode | Baseline median ops/s (range) | Shared-counter candidate median ops/s (range) |
| ---: | --- | ---: | ---: |
| 1 | EMDA disabled | 2,650,284 (2,300,933–3,217,471) | 2,417,307 (2,019,063–3,099,344) |
| 1 | EMDA enabled | 2,441,338 (1,840,328–3,128,646) | 2,460,820 (1,987,318–3,219,543) |
| 4 | EMDA disabled | 1,939,642 (1,916,979–1,997,342) | 1,984,136 (1,943,169–2,034,778) |
| 4 | EMDA enabled | 1,955,440 (1,901,031–2,046,750) | 1,910,243 (1,864,118–1,957,180) |

SQLite measured 987,699 ops/s at one client (976,554–1,005,688) and
1,393,577 ops/s at four (1,361,257–1,431,258). These are tiny
closed-loop warm-cache read workloads on one development host, with
Linux VM validation during the campaign. Each timed read checks the
expected contents. The run-level ranges overlap for both EMDA
revisions in all four cases. The results give mixed changes across
workloads. The workload does not measure admission saturation,
concurrent handoff, write durability, or process memory.

The [raw archive](raw-results.tar.gz) includes every command, exit
status, original benchmark output with latency quantiles, binary
hashes, the candidate diff and the runner. To repeat, build the baseline
and patched revisions into separate release targets. Extract the
archive, set `PYTHONPATH` to the repository root, and run
`python3 artifacts/paired.py <run-directory>` with the same
`baseline-target`, `candidate-target`, `fixtures` and `artifacts`
layout. Start with fresh fixture and output directories.
