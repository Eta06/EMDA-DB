# Contributing

The product name is a placeholder. The repository contains an experimental storage engine; compatibility, on-disk format, API, and future language interfaces may change until they are versioned and stabilized.

Use Rust 1.85 or newer. Before submitting changes, run:

```sh
cargo fmt --all -- --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --locked
cargo test --release --locked
python3 -m unittest discover -s scripts/tests -v
```

Keep the lockfile checked in. Add tests for behavior and failure modes, particularly changes to persistence, concurrency, recovery, or public APIs. Explain any change to durability or on-disk compatibility. Run the same workload and durability settings when reporting performance; include the environment, code revision, methodology, and full results. Do not report synthetic or unverified comparisons as fact.

No performance optimization may violate the durability, isolation, or recovery guarantees in force, including the prototype's documented behavior. Follow the [storage and transaction contract](docs/STORAGE_TRANSACTION_CONTRACT.md) and include relevant correctness and fault evidence with optimization changes.

Keep the README and the relevant architecture, roadmap, and feature inventory documents current when behavior, support, limitations, or setup commands change. Mark planned features separately from implemented and verified features.

Record user-facing changes in the `Unreleased` section of [CHANGELOG.md](CHANGELOG.md), and follow [the versioning policy](docs/VERSIONING.md) when preparing a release. Release tooling requires Python 3.11 or newer; it uses only the standard library.

The current engine is a prototype and must not be used for valuable or sensitive production data. Please report potential security issues privately to the project owner instead of publishing exploit details before a coordinated fix; a public security reporting channel is not yet configured.
