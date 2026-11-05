# Array-element scan and scalar comparison — 2026-11-05

The new `ArrayElementMatch` requires multiple nested conditions to match the same object. A local single-client scan compares the prototype with SQLite 3.49.1 JSON1 using documents with two objects: one red object and one blue object. A document matches only when the **red** object has a quantity of at least five. Unmatched documents deliberately put the larger quantity in the blue object. Every result is decoded and checked; all content and the matching query are checked again after reopening. Both implementations perform complete scans. The SQLite plan is recorded and verified; there is no multikey index.

An initial [four-round comparison](../benchmarks/results/2026-11-05-array-element-comparison.json) on revision `3b63121` found SQLite faster on all three sampled workloads. Code inspection identified that each prototype scalar comparison allocated two encoded buffers. Revision `209a62b` compares a validated scalar directly, preserving the index encoding order. Two [paired](../benchmarks/results/2026-11-05-array-element-paired.json) [runs](../benchmarks/results/2026-11-05-array-element-paired-repeat.json) rotate fresh before/after processes; each process also alternates prototype and SQLite queries. Each row below pools 400 timed samples per variant from four rounds. Values are **p50 / p99 microseconds**, nearest-rank:

| Run | Documents / matches | Prototype before | Prototype after | SQLite in after process |
| --- | ---: | ---: | ---: | ---: |
| Initial pair | 100 / 10 | 134.67 / 764.48 | 127.85 / 287.52 | 95.76 / 190.82 |
| Initial pair | 1,000 / 10 | 1,349.04 / 2,383.59 | 1,267.45 / 1,736.68 | 888.50 / 1,350.67 |
| Initial pair | 1,000 / 100 | 1,380.34 / 2,335.38 | 1,307.83 / 1,891.73 | 971.69 / 1,444.06 |
| Repeat | 100 / 10 | 134.60 / 307.62 | 129.12 / 288.73 | 95.42 / 269.51 |
| Repeat | 1,000 / 10 | 1,348.87 / 2,014.00 | 1,259.48 / 2,117.68 | 882.65 / 1,343.77 |
| Repeat | 1,000 / 100 | 1,381.37 / 2,488.17 | 1,306.88 / 2,327.02 | 967.18 / 1,455.56 |

The prototype's sampled median decreased approximately 4–7% across these cases and both pairs; SQLite remained faster. Tails varied: the initial 100-document prototype p95 increased from 212.82 to 226.34 microseconds, and the repeat 1,000/10 p99 increased. All 60 process runs across the initial and paired comparisons completed and verified content. The first pair had 22/24 quiet-window checks pass and at most 4.76% VM steal and 4.55% I/O wait; the repeat had 24/24 quiet checks pass but observed up to 11.45% steal and 7.69% I/O wait during measurement windows. Neither pair recorded cgroup CPU throttling. VM/cgroup telemetry includes setup, verification and other activity; the raw files retain every sample and all noise observations.

The optimization has a test comparing its results to the encoded index order across mixed types, signed integer extremes, UTF-8 strings and embedded zero bytes, plus non-indexable inputs. Planner and nested-array tests check invalid values and transactional behavior. [Model fuzzing](../benchmarks/results/2026-11-05-array-element-fuzz.json) on the preceding implementation and [three later scalar-comparison model campaigns](../benchmarks/results/2026-11-05-scalar-comparison-fuzz.json) exercised a further 38,082 inputs on revision `d740e20` without a finding. These finite serial campaigns do not exhaust inputs or concurrent schedules.

The prototype and SQLite run in the same release binary, pinned to one CPU, on one Linux VM and workspace overlay. SQLite uses WAL and `synchronous=FULL`; prototype writes use the witnessed synchronized store. The timed operations are read-only and warm-cache. SQLite JSON1 and the prototype differ on general types and validation, although the fixture uses matching semantics. Setup, transaction commits and reopen verification are outside operation timers. The benchmark does not measure concurrency, write latency, cold cache, general MongoDB/PostgreSQL workloads, physical power loss or off-host recovery; it is not a database ranking.

To reproduce after building release binaries from the desired before and after revisions:

```sh
cargo build --locked --release --example array_element_bench
python3 scripts/array_element_before_after.py --directory /path/on/test/filesystem \
  --before /path/to/before-binary --after /path/to/after-binary \
  --output /tmp/array-paired.json --rounds 4 --cycles 100
```
