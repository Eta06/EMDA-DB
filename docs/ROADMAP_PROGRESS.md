# Roadmap progress on 2026-11-27

The project aim covers all eight [roadmap steps](ROADMAP.md). The current
validation sequence prioritizes the macOS and Linux baseline, durable
storage and bounded recovery, and document correctness. One physical power
cut has been performed on one configuration, a Raspberry Pi 5 with NVMe on
ext4, during acknowledged atomic writes. An off-host restore of an ordinary
`Store` backup has been demonstrated. Other configurations, boundaries and
backup modes have not been tested.

| Step | Area | Progress | Basis and remaining evidence |
| --- | --- | ---: | --- |
| 1 | Correctness and foundations | 99% | 119 of 120 checklist items. Serializable transactions have been compared with SQLite and PostgreSQL, and resources measured on the R1 reference deployment. The open item is the standing contract-alignment obligation. |
| 2 | Durable storage and core operations | 99% | 181 of 183 checklist items. Retained rotation is occupancy-driven and off the writer path, and was killed at each lifecycle point on R1. The two open items await step 5. |
| 3 | Document model and queries | 99% | 115 of 116 checklist items. Retained snapshots share bounded aggregation and pagination. Project isolation, which awaits step 5, is the open item. Checkpoint-backed transactions do not yet share syncs. |
| 4 | SQL | 69% | Scored on `main` on 2026-11-28: 2.75 of the four [step 4 milestones](ROADMAP.md#priority-3-relational-and-sql-interfaces). **Parsing, planning and the documented CRUD subset: 1.** [`SQL_SUBSET.md`](SQL_SUBSET.md) documents the grammar, types, bounds and errors, and SQLite-oracle, crash, concurrency and fuzz tests cover it. **Joins, aggregates, sorting, indexes, transactions and diagnostics: 0.75.** Each has a tested, measured increment: `INNER`/`LEFT` joins of up to eight tables, with a first `RIGHT` join; aggregates, `GROUP BY`/`HAVING`, `ORDER BY`/`LIMIT`/`OFFSET`; online `CREATE [UNIQUE] INDEX` (1–4 columns, 16 per table); serializable multi-statement transactions (1,024 staged keys, 16 MiB); and rule-based `EXPLAIN SELECT`. `FULL` joins and later `RIGHT` joins, aggregates over joins and cost-based diagnostics are open. **Tables, typed columns, constraints, keys and migrations: 0.5.** `PRIMARY KEY`, `NOT NULL`, `UNIQUE`, literal column `DEFAULT`s, `CHECK` constraints, single-column `FOREIGN KEY` (`NO ACTION`/`RESTRICT`, `ON DELETE CASCADE`/`SET NULL`, up to 64 per table), `DROP TABLE` and `ALTER TABLE ... ADD COLUMN` are tested. Composite keys, `ON UPDATE` actions, expression defaults, and other `ALTER TABLE` actions are open. **Broader SQL with behavior tests: 0.5.** Uncorrelated `[NOT] IN`, `[NOT] EXISTS` and scalar subqueries (depth 4, at most 32,768 materialized values by default) match SQLite in differential tests. Correlated subqueries, derived tables and CTEs are open. The ordinary `Store` limits a transaction snapshot to 16,384 keys, which caps SQL tables there; `CheckpointStore` does not. |
| 5 | Secure service and project isolation | 10% | One of five milestones has partial evidence from the loopback benchmark adapter. A secure API, project isolation and safe scale-to-zero remain to build. |
| 6 | Performance and scalability | 60% | The comparative benchmark milestone has evidence. Profiling, durable group commit, hardware acceleration and scheduling each have partial evidence. Full workload and project-level scaling remain open. |
| 7 | Replication and operations | 20% | Local backup and restore, and offline format upgrades and imports, partially cover two of five milestones. Replication, failover, point-in-time recovery, distributed tests and rolling upgrades remain open. |
| 8 | Broader features and tooling | 10% | Query-plan explanations and recovery guidance partially cover one of five proposed differentiators. Recommendations, migration analysis and resource-aware tooling remain open. |

Steps 1–3 count checked bullets in the [foundation and storage](FOUNDATION_FOLLOW_UP.md)
and [document](DOCUMENT_FOLLOW_UP.md) checklists, including both drills
that were previously deferred. Their combined count is 415 of 419 (99.0%). Each item has equal
weight in that count. Steps 4–8 are estimates from roadmap milestones with
one point for demonstrated delivery, half a point for a tested subset, and
zero for design or prerequisites. Step 4 also uses quarter points, so that a milestone
with most but not all of its listed areas tested counts 0.75. The projects have different scope and the
percentages are neither effort estimates nor release-readiness gates.

### Steps 1–3 milestone accounting

The denominator has not changed for this milestone. All four open items stay
in it:

| Item | Checklist | Why it is open | Downstream acceptance |
| --- | --- | --- | --- |
| Keep acknowledgement, unknown outcomes, corruption and compatibility aligned with the contract | [Foundation](FOUNDATION_FOLLOW_UP.md), priority 0 | A standing obligation for every behavior change. The 2026-11-26 audit is recorded under it. | None; it stays open while behavior changes. |
| Request and connection limits, fair overload handling | [Foundation](FOUNDATION_FOLLOW_UP.md), priority 1 | The embedded admission, cancellation and deadline controls exist. Requests and connections need a server. | [Step 5](ROADMAP.md#priority-4-secure-service-and-project-isolation): per-project request and connection control, overload tests |
| Project-wide memory and storage quotas | [Foundation](FOUNDATION_FOLLOW_UP.md), priority 1 | Per-store disk budgets and per-catalog quotas exist. A project-wide quota needs a project. | [Step 5](ROADMAP.md#priority-4-secure-service-and-project-isolation): per-project memory, storage and background-work control |
| Authenticated service and project isolation for documents | [Document](DOCUMENT_FOLLOW_UP.md) | There is no service boundary to test locally. | [Step 5](ROADMAP.md#priority-4-secure-service-and-project-isolation): isolation at each layer, cross-project and unauthorized access tests |

Every other Steps 1–3 item is checked, each with linked evidence and
stated limits. The R1 measurements are reference results, not
production measurements. The power-cut drill covers one configuration.

The [benchmark catalog](BENCHMARKS.md), [isolation design](PROJECT_ISOLATION.md),
[scale-to-zero design](SCALE_TO_ZERO.md) and [storage contract](STORAGE_TRANSACTION_CONTRACT.md)
describe the evidence and boundaries in more detail.
