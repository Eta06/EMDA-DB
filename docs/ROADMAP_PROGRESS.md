# Roadmap progress on 2026-11-26

The project aim covers all eight [roadmap steps](ROADMAP.md). The current
validation sequence prioritizes the macOS and Linux baseline, durable
storage and bounded recovery, and document correctness. Physical power-loss
testing remains deferred until disposable physical hardware with power
control is available. An off-host restore of an ordinary `Store` backup has
been demonstrated; other backup modes have not.

| Step | Area | Progress | Basis and remaining evidence |
| --- | --- | ---: | --- |
| 1 | Correctness and foundations | 97% | 115 of 119 local checklist items. Full-log backup, restore, and format upgrade have scan ceilings. Broader fault and resource evidence remains open. |
| 2 | Durable storage and core operations | 96% | 176 of 183 checklist items. Retained recovery and startup retirement share a scan ceiling. Broader writable-memory and reclamation evidence remains open. |
| 3 | Document model and queries | 97% | 113 of 116 checklist items. Retained snapshots share bounded aggregation and pagination. Richer operators, transaction scalability, isolation and comparable server-engine measurements remain open. |
| 4 | SQL | 0% | None of four SQL milestones has a working prototype. The embedded transaction engine is a prerequisite shared with earlier steps. |
| 5 | Secure service and project isolation | 10% | One of five milestones has partial evidence from the loopback benchmark adapter. A secure API, project isolation and safe scale-to-zero remain to build. |
| 6 | Performance and scalability | 60% | The comparative benchmark milestone has evidence. Profiling, durable group commit, hardware acceleration and scheduling each have partial evidence. Full workload and project-level scaling remain open. |
| 7 | Replication and operations | 20% | Local backup and restore, and offline format upgrades and imports, partially cover two of five milestones. Replication, failover, point-in-time recovery, distributed tests and rolling upgrades remain open. |
| 8 | Broader features and tooling | 10% | Query-plan explanations and recovery guidance partially cover one of five proposed differentiators. Recommendations, migration analysis and resource-aware tooling remain open. |

Steps 1–3 count checked bullets in the [foundation and storage](FOUNDATION_FOLLOW_UP.md)
and [document](DOCUMENT_FOLLOW_UP.md) checklists, excluding the deferred
physical power-loss drill. Their combined count is 404 of 418 (96.7%). Each item has equal
weight in that count. Steps 4–8 are estimates from roadmap milestones with
one point for demonstrated delivery, half a point for a tested subset, and
zero for design or prerequisites. The projects have different scope and the
percentages are neither effort estimates nor release-readiness gates.

The [benchmark catalog](BENCHMARKS.md), [isolation design](PROJECT_ISOLATION.md),
[scale-to-zero design](SCALE_TO_ZERO.md) and [storage contract](STORAGE_TRANSACTION_CONTRACT.md)
describe the evidence and boundaries in more detail.
