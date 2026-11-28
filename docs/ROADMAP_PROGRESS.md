# Roadmap progress on 2026-11-28

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
| 4 | Embedded SQL subset | 100% | The four [SQL milestone](ROADMAP.md#priority-3-relational-and-sql-interfaces) areas are covered for the [documented subset](SQL_SUBSET.md): typed CRUD and planning; joins, aggregates, indexes, transactions and diagnostics with bounded cost-based access selection; schema migrations and scalar/composite referential actions; and derived tables, bounded recursive CTEs and immediate outer-row correlation. Wider SQL grammar, further optimizer work and network service are future work. |
| 5 | Secure service and project isolation | 65% | The [self-hosted server](SELF_HOSTED_SERVER.md) exercises all three data interfaces over hybrid TLS, with project-scoped roles, separated stores, request admission and enforced log caps. Configurable idle closure releases project stores and limits simultaneous wakes. The Linux deployment runs separate project processes with distinct UIDs and cgroup CPU, memory and task caps. The Rust SDK covers all three data models and API keys can be scoped to KV prefixes, document collections and single SQL tables. Managed account and key lifecycle, multi-table and row-level SQL policies, additional SDKs, physical quotas, at-rest encryption and multi-host lifecycle remain open. See the [project threat model](PROJECT_ISOLATION.md). |
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
