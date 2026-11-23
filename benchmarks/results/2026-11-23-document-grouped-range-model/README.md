# Compound integer ranges and document maintenance models

The compound-range model compares planned results with complete scans
after generated document replacements, deletes and reopens. Two new
query modes generate disjoint integer runs under inclusive or exclusive
upper bounds. The oracle also checks that gap rows do not consume the
candidate budget. The inputs exercise ordinary and checkpoint writers,
both with and without a partial index, and include signed endpoints,
duplicate membership values and nulls rejected by the scalar bounds.

The initial seed check found a stale expectation in an existing
partial-checkpoint seed. Negating an absence test now proves presence,
so the planner can use the compound range. Updating that expected plan
preserves the full-scan oracle. The original failed seed and log are
retained. The production planner is from the commit in `revision.txt`.

## Linux ARM64 model campaigns

| Model | Executions | Elapsed seconds | Retained corpus inputs |
| --- | ---: | ---: | ---: |
| Compound range | 53,965 | 301 | 1,217 |
| Online index | 25,236 | 91 | 688 |
| Schema migration | 37,025 | 91 | 658 |

After the seed expectation fix, the campaigns completed without model
divergence. The compound corpus contains grouped queries in all four
backend and partial-index combinations, with signed minimum, signed
maximum and interior bounds. These counts come from the retained
corpus, which is a subset of executed inputs.

The fuzz binaries used coverage instrumentation in a development build
on Linux ARM64 tmpfs with pinned nightly Rust. AddressSanitizer was
disabled for these local runs. The hosted fuzz workflow also runs the
extended seeds in its scheduled campaign. On macOS, all 52 selected
compound membership, range, disjunction and maintenance interleaving
tests passed.

## Retained document histories

The current revision passed 1,024 serial-history checks on macOS APFS
and 1,024 on Linux ARM64 ext4. Each history starts six concurrent calls
and crosses an automatic catalog generation change. The checker validates
real-time ordering, busy and cancellation outcomes, and the recovered
document, index, raw-key mirror and acknowledged epoch. Both runs
passed all 1,024 rotation and reopen checks. The histories ran in a
release build on macOS and a debug build on Linux. Two earlier Linux
builds were killed while compiling the SQLite dependency under guest
memory pressure. The final build ran after the fuzz campaign released
its temporary build artifacts.

These histories cover clean reopens. Physical power loss and independent
off-host restore remain deferred.

```sh
EMDA_RETAINED_DOCUMENT_MODEL_HISTORIES=1024 cargo +1.85.0 test --locked \
  --test document_transaction_model \
  retained_document_histories_serialize_and_recover_across_reclamation \
  -- --exact --nocapture
```

## Reproduction

Use `revision.txt`, apply the included `model.diff`, and use the
tracked compound-range seeds from this report's commit. Build with

```sh
cargo +nightly-2026-11-03 fuzz build --dev --sanitizer none document_compound_range
```

Run the resulting libFuzzer executable with a scratch copy of those
seeds and `-max_total_time=300 -max_len=96 -timeout=10 -seed=20261123`.
For `document_online_index` and `document_schema_migration`, build with
the same options and run 90 seconds with a maximum input length of 64
bytes and their corresponding tracked seeds. The [raw archive](raw-results.tar.gz)
preserves the generated corpora, logs, hashes and a script for auditing
the grouped-case coverage in `corpus.tar.gz`.
