# Spill merge sizing for disjoint key ranges

A spill rebuild can calculate the exact temporary merge size from the
retained metadata when the input key ranges are disjoint. The merge still
verifies all input runs before replacing them. Overlapping ranges still
require a sizing scan because the newest entry for a repeated key replaces
the older ones. The exact byte ceiling includes the offset sidecar.
Over-limit merges can reject before reading the input runs again.

## Measured rebuilds

The [probe](spill_disjoint_probe.rs.txt) rebuilds the managed checkpoint from
65,536 acknowledged batch children with 64-byte keys and eight-byte values.
It uses 1,024-key chunks and four-way merges. One fixture has 65,536 distinct
increasing keys. The other repeatedly writes 4,096 keys, so early merge
ranges can be disjoint but later ones overlap. The Linux ARM64 VM uses tmpfs
and V3 metadata witnesses. Both binaries used Rust 1.85.0, the unoptimized
profile, and the same source log for each workload.

| Workload | Baseline read bytes | New read bytes | Reduction |
| --- | ---: | ---: | ---: |
| Distinct increasing keys | 186,884,776 | 130,809,704 | 30.0% |
| Repeated keys | 190,020,674 | 171,324,482 | 9.8% |

Read counts are the `rchar` delta across the rebuild in a fresh process,
excluding fixture setup and final reopen. They include buffered and cached
reads and do not represent bytes read from a physical device. The three
repetitions per binary and fixture reported identical read counts. The
[raw log](results.log) also records read calls and timings. Timings from
this single tmpfs VM do not establish deployment latency.

Every trial verified ordered keys and values after a managed reopen and
left the source log byte-identical. Baseline and changed checkpoint SHA-256
values matched on each fixture. The macOS spill regression checks exact and
one-under limits with reverse-order disjoint ranges, including input
corruption at the admitted limit. Linux focused integration tests cover V3
witness preservation and the differential rebuild model.

## Reproduction

Copy the probe into `examples/spill_disjoint_probe.rs` in disposable
checkouts of baseline `bf39f2d` and the commit that adds this report. Build
both with `cargo +1.85.0 build --locked --example spill_disjoint_probe`.
Create two fresh fixtures with `PROBE NEW_FIXTURE setup KEYS`, where
`KEYS` is 65536 or 4096. For each run copy that fixture's `store` into a
new trial directory with `cp -a` to retain the witness, then run
`PROBE TRIAL_DIR VARIANT KEYS`. Compare checkpoint hashes across variants.
