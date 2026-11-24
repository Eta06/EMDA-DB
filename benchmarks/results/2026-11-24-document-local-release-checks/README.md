# Local document release checks

A clean source extraction of `21802da` passed 79 tests across eight
integration suites on macOS ARM64 with Rust 1.85.0. The suite covers document
CRUD and indexed queries, cumulative read limits, concurrent transaction
histories, interleaved maintenance, and recovery on the ordinary,
checkpoint and retained paths. The [raw test output](macos-tests.log) and
its [checksum](SHA256SUMS) are retained.

```sh
cargo +1.85.0 test --locked --quiet --test documents \
  --test document_transaction_model --test document_recovery \
  --test document_retained --test document_range_planner \
  --test document_compound_range_planner --test document_read_limits \
  --test document_maintenance_interleaving
```

Linux validation of the same Rust sources passed on both toolchains in
[CI](https://github.com/Eta06/EMDA-DB/actions/runs/45342929537). The
[cross-engine smoke archive](../2026-11-24-source-log-read-reservation/README.md)
preserves compound-range, concurrent-document and process-crash cases from
that revision. A separate [measured contention comparison](../2026-11-24-cross-engine-document-contention/README.md)
at `6cea60f` records repeated runs and raw samples against SQLite,
PostgreSQL and MongoDB.
