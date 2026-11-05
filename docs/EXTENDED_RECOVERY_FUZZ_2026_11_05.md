# Extended recovery and document fuzzing — 2026-11-05

Four coverage-guided campaigns ran on revision `4a2df54` in the Linux cloud VM, using the repository's existing seeds and address sanitizer. Each target had a 180-second libFuzzer budget and ran concurrently with the other three. All four completed without a finding:

| Target | Generated executions | Maximum input length |
| --- | ---: | ---: |
| `recovery` | 275,444 | 8,192 bytes |
| `checkpoint_store` | 15,489 | 128 bytes |
| `document_online_index` | 16,705 | 64 bytes |
| `document_schema_migration` | 21,882 | 64 bytes |

The [results file](../benchmarks/results/2026-11-05-extended-recovery-fuzz.json) contains exit status, fuzzer-reported runtime, log hashes and the end of each log. Driver elapsed times include compilation, waiting and polling, rather than only fuzzer execution. Isolated temporary copies of the seed corpora were used. With the applicable Rust nightly and `cargo-fuzz` installed, a target can be repeated using the corresponding directory from `fuzz/`:

```sh
cargo +nightly-2026-11-03 fuzz run TARGET CORPUS_COPY -- \
  -max_total_time=180 -max_len=MAX_LENGTH -timeout=10
```

For `recovery`, `checkpoint_store`, `document_online_index`, and `document_schema_migration`, the starting seed directories were respectively `fuzz/seeds`, `fuzz/checkpoint-store-seeds`, `fuzz/document-online-index-seeds`, and `fuzz/document-schema-migration-seeds`. Copy them first so the fuzzer can add cases without changing the repository.

These finite serial input campaigns do not exhaust malformed inputs, concurrent schedules, OS or device failures, physical power loss, or independent off-host restore. The separately documented remaining work is still open.
