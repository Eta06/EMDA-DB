# Intersecting document disjunction ranges

Revision `42c8c346` coalesces intersecting ranges on the same index and
leading equality tuple. The merged probe covers the exact union of the
encoded ranges. The residual filter still evaluates the original branches.
The executed range participates in read-set validation. Gaps, touching
exclusive endpoints, and exact equality probes keep their original checks.

## Regression coverage

- A projected 1 MB document matched by 34 overlapping ranges fits a
  one-candidate budget in queries and integer statistics. The previous
  revision rejects that budget after charging a repeated index entry.
- A compound union scans 260 candidates at the exact limit, resumes past a
  256-entry page, rejects one candidate short, and excludes a gap between
  disjoint ranges. The result matches the collection scan.
- Both ends of a merged range detect matching concurrent inserts and roll
  back a staged raw-key write. A concurrent insert outside the union lets
  that write commit. Full residual filtering and candidate admission still
  apply. Disjoint ranges still share the 32 MiB byte ceiling.
- The seeded differential probe compares 512 generated compound queries
  per backend with a full collection scan and an independent range model.
  It checks ordered IDs and the exact number of index candidates. On each
  backend, 367 of the 512 generated plans use fewer probes than branches.

## Cross-platform verification

Native macOS Rust 1.85 focused suites passed 63 tests. [Hosted macOS]
passed the full suites on both toolchains. [Hosted Linux] passed Rust 1.85,
stable, and the stable optimized suite. [Filesystem EIO], [ENOSPC], and
[cross-engine smoke] passed.

The [raw archive] preserves all 51 benchmark attempts, including five
cross-engine comparisons. The hosted million-key recovery workload
verified 53 phases, with 25 under a 48 MiB address-space ceiling. Logs,
model source, verification scripts, and a SHA-256 manifest are included.

[hosted Linux]: https://github.com/Eta06/EMDA-DB/actions/runs/45446141001
[hosted macOS]: https://github.com/Eta06/EMDA-DB/actions/runs/45446140960
[filesystem EIO]: https://github.com/Eta06/EMDA-DB/actions/runs/45446140953
[ENOSPC]: https://github.com/Eta06/EMDA-DB/actions/runs/45446140969
[cross-engine smoke]: https://github.com/Eta06/EMDA-DB/actions/runs/45446141071
[raw archive]: raw-artifacts.tar.gz
