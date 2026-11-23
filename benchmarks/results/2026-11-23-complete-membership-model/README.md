# Compound membership without range bounds

The compound-range differential model now generates complete
two-field membership filters without an explicit range. It covers
ordinary and checkpoint stores, with and without a partial index.
Histories interleave replacement, deletion, reopening and queries.
The model compares planned rows, order, projection, pagination and
match counts with the full collection scan. It also checks exact plan
selection and that successful index paths visit only the matching rows.

The new modes generate 256 exact probes, 256 grouped probes, a 258-probe
grouped product that must fall back, a gapped sparse list without any
consecutive pair, and an above-cap input that can fit in four grouped
probes. The generated scores include signed endpoints and duplicates.

The Linux ARM64 campaign completed **6,549 runs in 121 seconds** without
a differential failure or a generated reproducer. Coverage
instrumentation was enabled in a development build, with AddressSanitizer
disabled. The run included the original three-field range model.
The final corpus contains 894 inputs. The counts below measure retained
inputs that reach a new mode, including seeds, so one input can count in
several columns.

| Backend and index | Exact 256 | Grouped 256 | Grouped 258 | Sparse over cap | Four grouped probes |
| --- | ---: | ---: | ---: | ---: | ---: |
| Ordinary | 12 | 8 | 11 | 10 | 9 |
| Checkpoint | 192 | 153 | 107 | 113 | 118 |
| Ordinary partial | 22 | 17 | 15 | 14 | 15 |
| Checkpoint partial | 82 | 67 | 76 | 48 | 83 |

On macOS, all 48 selected compound membership, compound range and
disjunction tests passed. This campaign samples serial histories with
clean reopens. It does not measure concurrent interleavings or simulate
physical power loss.

## Reproduction

Extract [raw-results.tar.gz](raw-results.tar.gz). Use the repository
revision in `source-revision.txt`, apply `model.diff`, and copy the
new files from `seeds/` into `fuzz/document-compound-range-seeds/`.
Make a fresh corpus from the checked-in seeds, or replay the included
corpus. The original campaign command was:

```sh
mkdir -p new-corpus artifacts
cp fuzz/document-compound-range-seeds/* new-corpus/
CARGO_BUILD_JOBS=1 CARGO_INCREMENTAL=0 \
  cargo +nightly-2026-11-03 fuzz run document_compound_range new-corpus \
  --sanitizer none --dev -- -max_total_time=120 -max_len=96 -timeout=15 \
  -artifact_prefix=artifacts/
```

The archive includes the run log, exit status, revisions, build
environment, binary and model checksums, and the coverage counter.
