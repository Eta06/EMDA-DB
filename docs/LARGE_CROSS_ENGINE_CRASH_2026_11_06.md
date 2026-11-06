# Cross-engine process crashes with 100,000 keys

Two fresh-fixture rounds passed for the prototype, SQLite, PostgreSQL and
MongoDB on 6 November 2026. Each fixture contained 100,000 acknowledged seed
values, followed by 100 individually acknowledged inserts, 10 updates and 10
deletes. The test killed the database process or container with connections
still open, restarted it, then checked **every** seeded value, all 90 expected
later values and all 10 deletions. Across the eight runs, it verified 800,000
seeded values, 720 later values and 80 absences after 960 individual mutations.

The fixture used eight-byte keys and repetitive 64-byte values. Seed data was
committed in batches; later mutations were individual operations. The prototype
used a V3 witness with sync-before-acknowledgement; SQLite 3.49.1 used WAL and
`synchronous=FULL`; PostgreSQL 18.6 used `fsync`, `synchronous_commit` and
`full_page_writes`; MongoDB 8.0.34 used `w=1, j=true`. The adapters and engines
have different checksums, data formats and feature sets, so similar settings do
not establish identical durability guarantees.

## Recorded observations

The table records the first and second observations, not a performance
ranking. **Ready time is not an equivalent recovery measurement:** the
prototype and SQLite are embedded in loopback adapter processes, while the
other two start containers and use different readiness polling and connection
paths. PostgreSQL's process RSS is only its main process, excluding children;
embedded RSS includes the adapter, and container memory is a different
measurement. RSS excludes host page cache.

| Engine | Ready time, ms | Main/adapter RSS, MiB | Container memory snapshot |
| --- | ---: | ---: | ---: |
| Prototype | 39.0 / 49.7 | 14.1 / 14.0 | — |
| SQLite | 5.8 / 5.6 | 3.2 / 3.2 | — |
| PostgreSQL | 789.2 / 457.3 | 34.1 / 34.2 | 40.25 / 40.17 MiB |
| MongoDB | 2,749.6 / 2,802.5 | 233.2 / 230.1 | 237.0 / 237.1 MiB |

Fixtures and caches were warm on an Intel Xeon Platinum 8573C shared Linux VM
with a four-CPU aggregate quota and workspace overlay; the backing device was
not independently verified. Pre-run quiet checks passed. Restart intervals
showed up to 1.82% VM steal and no recorded CPU-quota throttling. Both MongoDB
restarts coincided with direct memory-reclaim scanning, so their timings are
particularly affected by interference. The [raw results](../benchmarks/results/2026-11-06-large-crash-comparison.json.gz)
retain all runs, resource scopes, file sizes and telemetry.

The driver ran from clean revision `6bf034dcdc0ed0a3fe2cf30ef6a379dcd43965b9`.
The Rust binaries were built with Rust 1.99.1; the Rust sources, examples and
Cargo files match that revision, and the result records binary hashes. The
driver does not independently establish build provenance from the binary hash.
The [hosted smoke workflow](https://github.com/Eta06/EMDA-DB/actions/runs/42171631087)
passed the same four-engine check with 128 seed keys.

For reproduction, build the three examples named by
`scripts/large_crash_compare.py --help`, install the pinned PostgreSQL and
MongoDB Python drivers and make the corresponding Docker images available.
Then use a disposable directory:

```sh
python3 scripts/large_crash_compare.py --directory /path/to/disposable \
  --output /path/to/new-result.json --keys 100000 --rounds 2
```

This is a **process-kill correctness check**, not physical power loss, an
in-flight crash, an off-host restore, a distributed fault, a full resource
quota test or a representative production benchmark. The OS and storage
caches remain running after the kill; two runs per engine and one fixture
shape cannot establish general performance or reliability rankings.
