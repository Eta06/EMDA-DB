# Changelog

Notable changes are recorded here. See [the versioning policy](docs/VERSIONING.md) for release and compatibility rules.

## [Unreleased]

The package version is currently `0.1.0`, but no version has been tagged or published yet.

### Added

- An experimental Rust key-value engine with `put`, `get`, and `delete` operations.
- Checksummed append records, synchronized writes, reopen recovery, and a single-process ownership lock.
- Concurrent-reader and serialized-writer coordination, plus tests for corruption, incomplete records, and abrupt process termination.
- GitHub Actions checks for formatting, linting, and tests on the minimum supported Rust version and stable.
- Architecture notes, a competitor feature inventory, a benchmark plan, and a prioritized roadmap.
- An attribution and notices file, versioning policy, and automated GitHub release workflow.
- A regression test that rejects unknown on-disk record versions without modifying the log.
- Instructions for trying the Apache-licensed Rust library from a Git dependency.
- A storage and transaction contract distinguishing current behavior from proposed guarantees, with a rule against optimizations that violate durability, isolation, or recovery.
- A README progress view and a prioritized assessment of proposed performance and database features.
- Design goals for safe project scale-to-zero, including draining, recovery, ownership, and cold-start testing.
- Project isolation threat boundaries, proposed protection layers and tiers, and validation requirements.

### Limitations

- This is a prototype, not a production database. SQL, document queries, multi-key transactions, compaction, bounded index memory, network access, and distributed features are not implemented.
