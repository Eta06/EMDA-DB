# Compaction handoff failure outcomes

Revision `ef7ec92114779853c82888f2fafd4d91575051bd` adds opt-in detailed
outcomes to the consuming ordinary and checkpoint-backed
`compact_and_replace` methods. They conservatively distinguish failure before
this call begins candidate publication, failure during candidate preparation
before this call begins directory exchange, and failure after exchange may
have begun. The methods consume the original handle even on failure. Existing
paths and temporary artifacts can remain; a candidate can be partly
configured, and no error returns a new checkpoint digest. Coordinate access
and reopen affected paths for inspection and reconciliation before retrying.
These classifications do not establish source health, completed cleanup or an
independent backup.

The ordinary handoff boundary test injects errors at preflight, after the
candidate copy, before exchange, after exchange and during parent sync. The
checkpoint-backed test covers those boundaries plus managed selection before
rename and before sync, with managed automatic checkpoints enabled. These 17
deterministic scenarios check classifications, whether the original log is at
the source or sibling path, and acknowledged values after reopening. An
integration test covers rejected existing destinations and successful handoffs
on V2/V3, including managed and unmanaged checkpoint stores. The V3 case is
skipped on filesystems that fall back to V2. The existing APIs retain their
previous errors and synchronization behavior.

Stable, Rust 1.85 and optimized stable all-target suites each passed 989 tests
with four test threads. Both all-target Clippy checks, formatting and 132
Python tests passed. These tests use selected synthetic hooks and a running OS;
they do not establish physical power-loss safety, independent off-host
recovery or every filesystem failure ordering. Online compaction and other
maintenance operations still have separate error boundaries.

A [filesystem-fault campaign](HANDOFF_FILESYSTEM_FAILURES_2026_11_14.md)
checks selected actual syscall errors and recovery through both paths.
