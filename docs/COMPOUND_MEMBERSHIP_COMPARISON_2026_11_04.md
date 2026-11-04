# Compound membership query comparison

A planned document query can now union exact probes on a compound index when every indexed field has a required scalar equality or `In` predicate. Values are deduplicated before forming complete tuples, with a maximum of 256 probes. Candidate-document and byte budgets span all probes, and each probe participates in the transaction's existing read-set validation. An implied partial index can be used; incomplete, non-indexable or excessive products cannot use this path, and a query without an earlier eligible path falls back to a full scan. This does not implement array-element matching, general boolean unions, cost-based planning or MongoDB compatibility. [Document semantics](DOCUMENT_MODEL.md) describes the restrictions.

Both writable backends have tests for unique and nonunique indexes, four-field tuples, null and missing values, duplicate inputs, partial guards, sorting and projection, exact probe and candidate bounds, multiple pages, cumulative byte limits and rollback, corrupt later-probe entries, staged writes, concurrent phantoms and recovery. Four [model and differential fuzz campaigns](../benchmarks/results/2026-11-04-compound-membership-fuzz.json) exercised 36,041 inputs without a finding at revision `5d079cc`; revision `4a53c2a` adds the four-field regression test without changing the implementation. Finite fuzzing is not exhaustive.

## Single-client read workload

The [benchmark](../examples/compound_membership_bench.rs) evaluates two two-value predicates on a complete two-field tuple, producing four distinct probes. The prototype compares its complete scan with the planned path. SQLite 3.49.1 uses JSON text, a forced expression index for the indexed variant, and a forced primary-table scan for the scan variant. SQLite's recorded plan also uses a temporary B-tree for ID ordering. Both engines parse and verify all returned JSON rows on every query; the adapters, internal storage and transaction implementations differ. Both datasets are prepared with durable writes; SQLite uses WAL and `synchronous=FULL`. The timed operations are reads only.

Each cell is the median of four process medians, each with 50 timed queries after ten warmups, in microseconds. The values are not pooled percentiles.

| Run | Documents / matches | Prototype scan | Prototype planned | SQLite scan | SQLite indexed |
| --- | ---: | ---: | ---: | ---: | ---: |
| tmpfs first | 100 / 10 | 94.59 | 26.63 | 53.41 | 18.51 |
| tmpfs repeat | 100 / 10 | 95.88 | 27.51 | 53.72 | 18.97 |
| workspace overlay | 100 / 10 | 101.60 | 28.09 | 54.19 | 18.75 |
| tmpfs first | 1,000 / 10 | 927.42 | 31.01 | 458.00 | 23.76 |
| tmpfs repeat | 1,000 / 10 | 932.84 | 31.39 | 457.91 | 23.34 |
| workspace overlay | 1,000 / 10 | 979.45 | 31.75 | 456.26 | 22.91 |
| tmpfs first | 1,000 / 100 | 937.22 | 216.20 | 533.25 | 115.74 |
| tmpfs repeat | 1,000 / 100 | 939.49 | 211.43 | 530.52 | 114.55 |
| workspace overlay | 1,000 / 100 | 986.21 | 220.49 | 533.21 | 113.70 |

The planned path reduced this prototype's scan time, while SQLite's indexed path was faster in each sampled case. These results do not rank general database performance, concurrency, writes or recovery. At most 1,000 small documents were used; results are warm-cache, single-process and single-client, with both datasets in the same process. Setup and full reopen verification are outside timers. CPU affinity and quiet windows were used, but two of 36 quiet checks failed and individual samples reported up to 9.1% VM steal. The workspace overlay's backing device is unknown.

The [first](../benchmarks/results/2026-11-04-compound-membership-primary.json.gz), [repeat](../benchmarks/results/2026-11-04-compound-membership-repeat.json.gz) and [overlay](../benchmarks/results/2026-11-04-compound-membership-overlay.json.gz) records contain raw samples, plans, revisions, binary hashes and VM/cgroup telemetry. Use [the comparison driver](../scripts/nonunique_query_compare.py) with `--index-name docs_pair --compound-membership` to repeat the workload.
