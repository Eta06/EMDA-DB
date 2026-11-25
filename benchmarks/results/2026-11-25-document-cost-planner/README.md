# Opt-in cost-based choice among document range paths

Revision `2ef00f61` adds `query_planned_estimated` and
`explain_query_estimated`. When more than one range-class index path is
semantically eligible, they count each candidate's index entries and choose
the smallest. Default `query_planned` and `explain_query` are unchanged and
still take the first eligible path lazily. This is one slice of the open
document cost-planner criterion. Statistics, other access classes and
broader cost modelling remain open.

## Failure case

The default range pass takes the first eligible single-field range in
index definition order. Take a filter `age >= 0 AND score >= N - 10` with
`age` defined first and present on every document. The default plan reads
every document as a candidate. Planned queries admit at most 1,024
candidates, so above that size the default plan fails with `document scan
exceeds the operation limit`, although the `score` index needs ten.

## Bounds and semantics

- **Global budgets.** `DocumentEstimateLimits` sets a total entry budget and
  a total scan-call budget across all candidates. Each later candidate
  counts only to one less than the best count so far, and each count stops
  at the remaining budget. Every scan call is charged, including one that
  returns nothing; an empty key range needs no call. The estimates report
  `entries_read` and `scans`, which never exceed the limits. A count cut
  short by either budget cannot win.
- **Fallback.** Ties, inconclusive counts and exhausted budgets keep the
  default order, so the choice is deterministic for the same view.
- **Transaction effects.** Estimate reads go through the transaction's
  admitted scan. They see staged writes, join its read set and count
  against its read limits, so they can add a conflict on an index that is
  not chosen. That is why the cost choice is opt-in.
- **Correctness.** Every candidate is an existing eligible path, and the
  filter is reapplied, so results do not depend on the estimate.

## Tests

Eight tests in `tests/document_cost_planner.rs` run on ordinary,
checkpoint-backed and managed retained stores:

- Scan parity for the matched case. The default plan reads all 400
  candidates and fails under a 20-candidate budget. The estimated plan reads
  ten candidates, 267 estimate entries and three scan calls. The
  explanation matches.
- Entry budgets from 0 to 10,000. The choice switches to the narrow index
  only once the budget can prove it narrower, and entries stay within
  budget.
- Staged rows that widen one range and match both. The estimates count
  them, and results match a transactional scan.
- A read limit that admits the narrow probe alone but not the estimate.
- A read-set transaction that conflicts with an insert only in the
  unchosen range when estimating, and does not conflict without it. This
  runs on ordinary and checkpoint stores.
- Sixty-four empty membership seeks that exhaust a 16-call scan budget and
  fall back. With room, the empty candidate wins after 66 calls.
- An empty first candidate stops further estimation after one call.
- A single eligible range that is not estimated.

The existing document planner suites and the document library tests
passed unchanged. The full native macOS suite passed 1,522 tests
(`full-tests.log`), and warnings-denied Clippy passed. On `2ef00f61`, [CI]
passed Linux Rust 1.85, stable, optimized stable and the memory-ceiling
job. [macOS] passed both toolchains, and the [EIO], [ENOSPC] and
[cross-engine smoke] workflows passed. The eight tests passed in every
hosted test job.

## Matched measurement

`examples/document_cost_planner_bench.rs` indexes `age` and `score` on
every document. It runs the query above through both planners on an
ordinary store and on a checkpoint-backed copy, and checks results against
the exact expected documents. A complete scan is also checked when one
fits. Logical work was identical in all three trials of each size. The
binary's engine source matches the commit (`candidate-source.diff`,
`committed-source.diff`).

| Documents | Default plan | Estimated plan |
| ---: | --- | --- |
| 1,000 | `age`, 1,000 candidates | `score`, 10 candidates + 267 estimate entries in 3 scans |
| 4,000 | fails at the 1,024-candidate limit | `score`, 10 candidates + 267 estimate entries in 3 scans |

Median elapsed time for the 1,000-document query was 1.7 ms for the default
plan and 0.06 ms for the estimated plan on the ordinary store. On the
checkpoint store it was 37.2 ms and 0.40 ms. These are single-host warm
readings on an Apple M6 with macOS APFS and Rust 1.100.0.

Two failed attempts are kept:

- `failed-10000-snapshot-key-limit/`: 10,000 documents exceeded the ordinary
  store's 16,384-key transaction snapshot cap.
- `failed-4000-scan-cap/`: an earlier binary requested a 4,000-candidate
  budget, above the 1,024-candidate query cap.

[CI]: https://github.com/Eta06/EMDA-DB/actions/runs/45571600377
[macOS]: https://github.com/Eta06/EMDA-DB/actions/runs/45571600279
[EIO]: https://github.com/Eta06/EMDA-DB/actions/runs/45571600250
[ENOSPC]: https://github.com/Eta06/EMDA-DB/actions/runs/45571600319
[cross-engine smoke]: https://github.com/Eta06/EMDA-DB/actions/runs/45571600257
[raw archive]: raw-artifacts.tar.gz

The [raw archive] holds every trial, the failed attempts, the binary hash,
the source diffs, the full test log and the hosted run logs.
