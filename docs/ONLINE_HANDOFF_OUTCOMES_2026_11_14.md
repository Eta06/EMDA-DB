# Online compaction handoff outcomes

Revision `6ef7986364f48119808f3ed7f4f1638a0f9f5c25` adds the opt-in
`OnlineStore::compact_and_replace_detailed` API. It conservatively
distinguishes failure before this call begins destination publication,
failure after candidate publication may have begun but before exchange is
attempted, and failure after exchange may have begun. The handle is not
consumed. An exchange-uncertain failure disables it; coordinate callers, drop
it, and reopen both paths to reconcile before retrying. The existing online
reclamation API retains its separate error type.

Other callbacks can write while the copy is prepared. A candidate left after
failure may omit later source writes and must not be assumed to be a current
backup. Pre-existing destinations or temporary stages may exist in any
case; classifications do not establish source health or completed cleanup.

Deterministic tests cover preflight, initial copy, the final barrier, candidate
publication, exchange and parent-sync failures. They verify the classification,
original-log location, handle behavior and acknowledged values after reopening.
A concurrent test acknowledges a write during preparation and confirms that a
later not-published result neither removes that write nor implies independent
operations did not occur. Integration checks cover rejected existing
destinations and successful handoffs on V2/V3; the V3 case skips if the
filesystem falls back to V2.

Stable, Rust 1.85 and optimized stable all-target suites each passed 993 tests
with four test threads. Both all-target Clippy checks, formatting and 135
Python tests passed. The checks use selected synthetic hooks on a running OS;
they do not establish physical power-loss safety, independent off-host
recovery or every concurrent schedule and filesystem failure ordering.
