# Cross-engine process restart check — 6 November 2026

Two fresh-fixture runs each passed for the prototype and ten comparison
configurations after their server process or container was killed. Each run
acknowledged 100 inserts, 10 updates and 10 deletes; after restarting the same
storage it verified 90 expected values and 10 expected absences. Across the 22
runs this is 2,640 acknowledged mutations, 1,980 checked values and 220
checked deletions. It is a correctness check, not a recovery-speed ranking.

| Configuration | Recorded version or build | Passing runs |
| --- | --- | ---: |
| Prototype | 0.1.0, V3 log | 2 |
| SQLite, Rust adapter | 3.49.1 | 2 |
| SQLite, Python adapter | 3.53.1 | 2 |
| PostgreSQL | 18.6 | 2 |
| Redis | 8.2.11 | 2 |
| MongoDB | 8.0.34 | 2 |
| Turso Rust engine, Python binding | pyturso 0.8.3 | 2 |
| RocksDB | 9.8.4 | 2 |
| LMDB | 0.9.36 | 2 |
| DuckDB | 2.0.0 | 2 |
| FoundationDB, single process | 7.3.79 | 2 |

The [raw results](../benchmarks/results/2026-11-06-cross-engine-process-crash.json.gz)
record clean source revision `4f079dec4ce0743b77d090a2c8cc2f0c9918c0f7`,
source hashes, container image identities, adapter metadata, dependency
versions, runtime and the prebuilt Rust adapter's SHA-256. The native adapter
was already built before the run; its hash identifies the executable, but the
recorded source revision alone does not prove its build provenance. Fixtures
were on the workspace overlay; its underlying storage device was not verified.
The [methodology](COMPARISON_METHOD.md) describes the distinct adapters and
durability settings. Similar settings do not establish identical guarantees.

As a provenance follow-up, the Rust adapter was rebuilt with Rust 1.99.1 from
clean revision `8a496b28bb7c24f2417345fbbc42a4a3165a2b25`; its new hash and
source hashes are recorded in a separate
[four-run result](../benchmarks/results/2026-11-06-rebuilt-adapter-crash.json).
Both prototype runs and both SQLite runs passed the same process-crash check.
The full 22-run matrix above still used the earlier prebuilt adapter.

An earlier [pilot](../benchmarks/results/2026-11-06-process-crash-pilot.json)
passed the first nine configurations but failed to load FoundationDB's missing
native client library. After installing the matching 7.3.79 library from the
already available container image, a separate
[FoundationDB retry](../benchmarks/results/2026-11-06-foundationdb-crash-pilot-retry.json)
passed; the two complete rounds above then passed. Both the failure and retry
are retained rather than silently omitted.

The kills occurred after all writes had returned, with open client connections.
They leave the OS and device caches running. This does not test interruption
at arbitrary commit boundaries, physical power loss, corruption, off-host
restore, concurrent client histories or distributed FoundationDB resilience.
The small dataset is not representative of every workload. For latency and
throughput, see the separate [local engine comparison](FOUNDATION_ENGINE_COMPARISON_2026_11_06.md),
including its limitations.
