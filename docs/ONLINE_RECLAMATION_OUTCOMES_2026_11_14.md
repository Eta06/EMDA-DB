# Online reclamation outcomes

Revision `70eda753ad5c901eb1f788d0ce5d672eec59f8e2` adds
`OnlineStore::compact_and_reclaim_detailed`. Its
`OnlineReclaimFailure::Compaction` contains the existing online handoff
classifications: this call did not begin candidate publication, candidate
publication may have begun, or exchange may have begun. An exchange-uncertain
outcome disables the handle; coordinate callers, drop it and reopen both
paths before retrying. Independent callbacks can write while preparation runs,
so a candidate can omit later writes and is not necessarily a current backup.

`OnlineReclaimFailure::Reclamation` instead means the synchronized handoff
succeeded, but cleanup failed; it includes the successful handoff report. The
old copy may be partially removed or retained under an owned staging name.
This cleanup failure alone does not disable the new live handle, though
independent operations can fail. Neither classification proves cleanup,
secure erasure, source health or an independent backup. The original
`compact_and_reclaim` method and its error type are unchanged.

Deterministic checks exercise preflight, initial copying, the final barrier,
candidate publication, exchange and parent-sync failures, with V2 and the
default local format. They check that reclamation has not started after a
failed handoff, that exchange uncertainty blocks reads and writes, and that
acknowledged data can be reopened. Ten cleanup hook boundaries on both starting
formats verify the separate reclamation result and a usable recovered live
store. A concurrent test acknowledges a source write during preparation and
checks it survives a later not-published outcome. Public API checks cover
invalid options, destination conflicts and successful reclamation; unsupported
platform behavior has a portable integration test whose macOS run is pending.

Stable, Rust 1.85 and optimized stable all-target suites each passed 1,004
tests; one guarded tmpfs ENOSPC test was ignored by each ordinary suite.
Both all-target Clippy checks and formatting passed. These hook checks sample
specific boundaries on a running OS; they do not prove physical power-loss
safety, off-host restore or every filesystem failure ordering.

## Reference model

The existing online-compaction model now alternates the original and detailed
reclamation methods, including rejected destinations and invalid options. A
90-second run completed **4,611 inputs** without a finding. The
[metadata and hashes](../benchmarks/results/2026-11-14-online-reclaim-model.json),
[raw log](../benchmarks/results/2026-11-14-online-reclaim-model.log.gz),
[saved corpus](../benchmarks/results/2026-11-14-online-reclaim-model-corpus.tar.gz)
and [replay log](../benchmarks/results/2026-11-14-online-reclaim-model-replay.log.gz)
are retained. Of 245 corpus files, 53 force the initial V2 format and 192 use
the default (V3 on this Linux filesystem). Replaying them completed 247
reported runs. The configured maximum input was 96 bytes, but the fuzzer
reached a 24-byte generation limit in this short run. Both new fixed seeds
passed individually.

The model is sequential and does not inject I/O failures, model concurrent
callbacks or establish physical power-loss behavior. The scheduled and
manually triggered fuzz workflow already runs this target. To replay the corpus
on a suitable Linux machine with the pinned nightly toolchain and `cargo-fuzz`,
extract it into a new empty `extracted-corpus` directory and run:

```sh
cargo +nightly-2026-11-03 fuzz run online_compaction ./extracted-corpus -- -runs=245 -max_len=96 -timeout=15
```
