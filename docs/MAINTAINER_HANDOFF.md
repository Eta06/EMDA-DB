# Maintainer handoff

## Ownership

Emir Tunahan Alim owns the project at [Eta06/EMDA-DB](https://github.com/Eta06/EMDA-DB).
Pınar is the lead maintenance agent. Pınar directs the work, reviews changes
made by agents, and owns the final verification, integration, and progress
report. A delegated Sol agent is a contributor to a specific task.

Use the repository's existing Git author configuration and GitHub remote.
Keep local integration work on `development` and publish coherent, verified
batches to `main`. The current long-lived local branches are `main`,
`development`, and `features`. Keep commits focused. Record user-facing fixes
and changes under `Unreleased` and follow the [version policy](VERSIONING.md)
for intentional releases. Preserve dated README snapshots and raw evidence.
The [November 22 README snapshot](../README_HISTORY.md) is the former front
page at commit `7fa0592`; Git retains the earlier revisions.

## Mission and current work

- Make EMDA-DB reliable on macOS and Linux. Complete the independently testable
  exit criteria for roadmap Steps 1–3 ahead of broader features.
- Preserve the [storage contract](STORAGE_TRANSACTION_CONTRACT.md), including
  acknowledgement, recovery, atomicity, and isolation guarantees.
- Build toward scale-to-zero with durable data, strong project isolation, and
  low latency, CPU, and RAM cost. Compare equivalent workloads with other
  databases under clearly documented durability settings.
- Report what was implemented, measured, and verified. Keep the physical
  power-loss test and independent off-host restore drill explicitly deferred.

The [roadmap](ROADMAP.md) defines the exit criteria. The
[integration handoff](INTEGRATION_HANDOFF_2026_11_16.md) and
[benchmark archive](../benchmarks/results) retain earlier engineering evidence.

## Delegation

Use agents when their assignments can run independently without colliding.
Use another GPT-6.2 Sol for substantial implementation, review, or scoped
defensive testing. Prefer Gemini Hydrogen for smaller research and idea work
when available. Consult Fable 5.2 when available and a difficult design or
performance decision warrants a second opinion. Assign each agent a clear
scope and stopping point. Review its findings and verify integrated changes
before publishing.

Link the relevant [roadmap section](ROADMAP.md#product-research-and-documentation)
or [idea brief](PERFORMANCE_AND_FEATURE_IDEAS.md) directly in product research
and documentation assignments. Once the shared SDK contract is agreed, delegate
Node.js and Python implementation separately and use the same compatibility
suite to review both.

## Development disk usage

Periodically check free space and growth in the checkout and validation
environments, particularly before large builds and fuzz or benchmark campaigns.
Inspect `target/`, `fuzz/target/`, temporary runs, and benchmark artifacts.
Preserve tracked evidence, reproducibility inputs, and artifacts used by active
jobs. Identify disposable build output before cleaning it up.
