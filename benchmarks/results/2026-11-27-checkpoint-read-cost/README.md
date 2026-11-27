# Checkpoint-backed indexed read, point read, and full-scan latency

Interleaved before/after measurements on one macOS APFS host comparing baseline
commit `d8254003` against the candidate optimizations:
1. Packed first-key prefix directory (up to 256 bytes per checkpoint block,
   bounded by `CheckpointViewLimits`, with full verified-block fallback on
   longer shared prefixes) eliminating intermediate binary-search block reads
   for keys sharing the 16-byte reserved document prefix.
2. Lazy, bounded default verified-block LRU cache on `CheckpointStore`
   (`DEFAULT_CHECKPOINT_BLOCK_CACHE_BYTES` = 8 MiB, clamped to
   `CheckpointViewLimits::max_file_bytes`, configurable down to `0`, standalone
   `CheckpointView`/`RecoveredCheckpointView` uncached by default), with lock
   hardening and limit preservation/cache clearing across rollover,
   `compact_and_replace`, and online handoff.
3. Scoped batched point-read sessions for index-entry to document fetches
   (`DocumentTransaction::visit_index_with_metadata`), reusing the loaded
   verified checkpoint block, state/read-set guards, and bounded log read-ahead
   across rows in a page.
4. Bounded 64 KiB ephemeral log read-ahead (within a 4 KiB gap) for scans,
   `get_many`, and batched point reads on `Store` and checkpoint-backed views,
   preserving per-value CRC-32 verification and admission boundaries.
5. Mathematically equivalent tree-bounds fast path in
   `CollectionSchema::validate_stored_document` (always enforcing every schema
   rule and full depth/node/size invariants).

## Host and environment

- OS: macOS 27.0 (Build `26A425`, `Darwin 27.0.0`, `RELEASE_ARM64_T8152`)
- CPU / RAM: Apple M6 (12 cores), 16 GiB RAM
- Storage: internal SSD, APFS (local mount reporting `MNT_LOCAL` and
  `VOL_CAP_INT_BARRIERFSYNC`; V2 barrier-ordered sync active on both baseline
  and candidate)
- SQLite reference: `rusqlite 0.35.0` / `libsqlite3-sys 0.33.0` (bundled
  SQLite, `journal_mode=WAL`, `synchronous=FULL`, `fullfsync=ON`)
- Toolchain: `rustc 1.100.0 (0ef1bc386 2026-11-09)`,
  `cargo 1.100.0 (3acc79c0c 2026-11-05)`, release profile (`--release --locked`)
- Concurrency and core-scheduling caveat: other developer builds/tests or macOS
  background services could run on this host during the measurement window, and
  Apple Silicon QoS scheduling can place threads on P-cores (~10 us per 32 KiB
  SHA-256) or E-cores (~48 us per 32 KiB SHA-256, especially after sustained
  `F_FULLFSYNC` sleeps). All suites ran 3 strictly interleaved trials with
  alternating order (`baseline, candidate` in trials 1 and 3; `candidate,
  baseline` in trial 2) to control for host drift.

## Profiling breakdown (baseline `d8254003`, 10,000 rows)

### 1. Checkpoint-backed indexed reads (`items1` + `items3`, 60,000 keys, 4.36 MB checkpoint, ~134 × 32 KiB blocks)

At baseline (`d8254003`):
- Every document key starts with `\0emda-document\x01D` and every index key
  starts with `\0emda-document\x01I` (16 identical bytes). Because the block
  directory retained only 16-byte first-key prefixes (`KEY_PREFIX_BYTES = 16`),
  every binary-search probe in `CheckpointLookup::preceding_block` was
  inconclusive and read + SHA-256-verified a full 32 KiB checkpoint block from
  disk (~6–7 wasted intermediate block verifications per lookup).
- `CheckpointStore`'s verified-block cache was disabled by default (`0`), and
  index-to-document fetches re-entered the store per row without reusing the
  loaded verified block.
- Consequently, 100 queries performed:
  - `pk_sql`: **863** block reads (8.63/query vs. 3 target keys)
  - `eq_sql`: **1,512** block reads (15.12/query vs. 4 target keys)
  - `range_sql` (10 rows): **7,422** block reads (74.22/query vs. ~4 distinct
    blocks)
- On Apple M6, a 32 KiB `Sha256::digest` takes ~10–15 us on a P-core and
  **~47.5 us on an E-core** (measured directly via `sha32k_post_ins`).
  Multiplying ~15 block hashes (`idx_eq`) or ~74 block hashes (`idx_range_10`)
  by ~10–48 us explains both the P-core (~173 us / ~828 us) and E-core
  (~746–785 us / ~3.67–3.80 ms) latencies, as well as the multi-millisecond
  E-core penalty on `insert_1idx`/`insert_3idx` (which check metadata, doc ID,
  and 1–3 index keys before appending).

### 2. Full 10,000-row predicate scan on `Store` (`log_bytes = 1,259,531`, `avg_value_bytes = 63.6`)

Baseline `full_scan_split_store` (median p50 across 3 trials):
- `raw_store_scan` (10 pages × 1,024 rows): **2,859.8 us (53.2% of `sql_full_select`)**
  - Sequential 64 KiB `pread` of entire 1.26 MB log (`seq_64k_pread`): **35.9 us (0.7%)**
  - CRC-32 over all 10,000 values (`crc_only`): **30.8 us (0.6%)**
  - Remaining **~2,793 us (~52%)**: 10,000 individual `pread(2)` syscalls
- `tx_rs_scan` minus `raw_store_scan` (`ReadSet` recording overhead): **+14.9 us (0.3%)** — negligible
- `doc_scan` minus `tx_rs_scan` (`DocumentTransaction::scan_documents`): **2,101.0 us (39.1%)**
  - `serde_json::from_slice` into `Value` (`json_parse_only`): **1,519.8 us (28.3%)**
  - Tree + schema validation, ID/key validation, allocations: **~581 us (10.8%)**
- `sql_full_select` minus `doc_scan` (`src/sql/`): **397.1 us (7.4%)**

## Interleaved before/after results (`d8254003` vs. candidate)

Ranges are `min..max` across the 3 interleaved trials with the median in
parentheses.

### 1a. `sql_bench <dir> 10000 100`

| Build | Backend | `pk_select` p50 (us) | `full_select` p50 (us) | `idx_eq` p50 (us) | `idx_eq` p99 (us) | `idx_range_10` p50 (us) | `idx_range_10` p99 (us) | `insert_1idx` p50 (us) | `insert_3idx` p50 (us) |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Baseline (`d8254003`) | `emda-store` | 4.6..26.9 (5.3) | 5,413.2..6,260.0 (5,462.7) | — | — | — | — | — | — |
| Candidate | `emda-store` | 5.2..21.9 (5.5) | **3,057.1..3,953.9 (3,146.2)** | — | — | — | — | — | — |
| Baseline (`d8254003`) | `emda-checkpoint` | 4.7..7.8 (6.8) | 5,496.3..5,624.7 (5,510.3) | — | — | — | — | — | — |
| Candidate | `emda-checkpoint` | 5.3..9.2 (5.5) | **3,090.1..3,630.5 (3,243.8)** | — | — | — | — | — | — |
| Reference (candidate runs) | `sqlite` | 2.5..8.0 (2.5) | 236.1..369.2 (249.4) | — | — | — | — | — | — |
| Baseline (`d8254003`) | `emda-checkpoint-indexed` | — | — | 172.7..181.8 (173.3) | 223.5..540.9 (303.8) | 812.9..870.3 (827.5) | 1,027.7..1,618.3 (1,137.0) | 4,014.5..10,864.1 (4,074.8) | 3,995.4..10,997.8 (4,002.5) |
| Candidate | `emda-checkpoint-indexed` | — | — | **9.0..13.8 (9.3)** | **45.2..58.1 (50.0)** | **21.3..29.0 (21.4)** | **55.6..72.2 (57.1)** | **2,998.5..3,969.5 (3,947.4)** | **3,042.5..4,040.1 (3,990.9)** |
| Reference (candidate runs) | `sqlite-indexed` | — | — | 2.6..4.5 (2.7) | 10.5 | 2.4..3.5 (2.4) | 9.5 | 2,992.5..3,087.7 (3,066.8) | 2,994.8..3,078.8 (3,029.0) |

(`emda-store-indexed` at 10,000 rows is skipped by `examples/sql_bench.rs` on
both baseline and candidate because 60,000 live keys exceed `Store`'s 16,384-key
snapshot cap.)

### 1b. `sql_bench <dir> 2000 100`

At 2,000 rows all keys fit in the suffix overlay without automatic rollover, so
`emda-checkpoint-indexed` is overlay-backed. Note that macOS scheduled the
shorter 2,000-row runs on slower cores for both baseline and candidate alike
(visible in `sqlite-indexed`: `13.5` vs. `13.7 us` `idx_eq` and `11.8` vs.
`11.8 us` `idx_range_10`).

| Build | Backend | `full_select` p50 (us) | `idx_eq` p50 (us) | `idx_range_10` p50 (us) | `insert_1idx` p50 (us) | `insert_3idx` p50 (us) |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Baseline (`d8254003`) | `emda-store` | 2,487.7..2,572.5 (2,532.2) | — | — | — | — |
| Candidate | `emda-store` | **1,654.4..1,755.7 (1,683.7)** | — | — | — | — |
| Baseline (`d8254003`) | `emda-checkpoint` | 2,449.8..2,521.7 (2,480.5) | — | — | — | — |
| Candidate | `emda-checkpoint` | **1,725.5..1,777.3 (1,752.1)** | — | — | — | — |
| Reference (candidate runs) | `sqlite` | 88.1..382.7 (382.6) | — | — | — | — |
| Baseline (`d8254003`) | `emda-store-indexed` | — | 35.8..38.9 (37.4) | 92.0..93.7 (92.1) | 5,030.3..5,925.5 (5,894.7) | 4,804.2..6,919.6 (6,047.1) |
| Candidate | `emda-store-indexed` | — | 37.4..40.8 (38.5) | **81.7..89.2 (82.0)** | 5,909.4..9,835.2 (5,925.8) | 6,917.6..9,286.7 (7,967.4) |
| Baseline (`d8254003`) | `emda-checkpoint-indexed` | — | 37.8..38.2 (37.8) | 96.4..99.6 (96.8) | 3,896.0..5,908.6 (5,858.6) | 4,027.0..6,108.2 (6,061.1) |
| Candidate | `emda-checkpoint-indexed` | — | 37.0..39.0 (38.9) | **87.0..91.6 (87.9)** | 5,876.1..5,919.9 (5,879.8) | 6,075.3..6,948.5 (6,114.5) |
| Reference (candidate runs) | `sqlite-indexed` | — | 13.0..13.8 (13.7) | 11.7..12.0 (11.8) | 5,023.0..5,162.5 (5,128.0) | 5,133.1..6,860.5 (5,962.8) |

### 2. Focused probe (`checkpoint_read_probe <dir> 10000 100`, 3 interleaved trials)

#### 2a. Steady-state checkpointed 10,000-row indexed tables (isolating prefix index + batching vs. verified-block cache)

| Mode | Build | `pk_sql` p50 (us) | `eq_sql` p50 (us) | `range_sql` p50 (us) | `full_sql` p50 (us) | Block misses (`pk` / `eq` / `range` / `full`) | Cached RAM |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `cache_0_off` (cache disabled) | Baseline | 79.9..175.5 (80.0) | 142.8..272.0 (145.2) | 789.7..799.0 (793.0) | 6,937.2..7,142.6 (7,112.2) | 863 / 1,512 / 7,422 / 1,155\* | 0 B |
| `cache_0_off` (cache disabled) | Candidate | **18.9..21.8 (21.4)** | **28.9..34.3 (30.4)** | **38.8..47.2 (40.2)** | **4,051.4..4,123.1 (4,076.7)** | **300 / 400 / 403 / 575\*** | 0 B |
| `cache_8m` (8 MiB default) | Baseline | 3.3..3.6 (3.4) | 4.8..5.1 (4.8) | 16.2..16.6 (16.2) | 5,755.3..6,063.8 (5,985.9) | 16 / 22 / 0 / 0 | 1.68 MiB (52 blk) |
| `cache_8m` (8 MiB default) | Candidate | **3.2..4.5 (3.3)** | **4.2..6.0 (4.3)** | **13.8..18.2 (13.9)** | **3,510.2..4,136.8 (3,514.1)** | 17 / 24 / 1 / 0 | **1.45 MiB (45 blk)** |

(\*Miss counts measured in the adjacent `cache_1_count_misses` phase with
`limit = 1`, which counts every block verification without storing blocks.
Directory size for the 134-block checkpoint rose from `28,672 B` to `32,768 B`,
i.e., +4 KiB.)

#### 2b. `sql_bench` managed-rollover repro with cache disabled (`exact_cache0`)

Even when a caller explicitly disables the block cache (`limit = 0`), the
256-byte prefix directory + batched point reader cut `idx_eq` from
`160.4..746.2 (169.1) us` to **`47.1..182.8 (47.2) us`** (3.6x faster) and
`idx_range_10` from `806.3..3,672.8 (849.8) us` to **`55.9..247.9 (59.8) us`**
(14.2x faster).

#### 2c. Full 10,000-row scan split on `Store` (`full_scan_split_store`)

| Metric (10,000 rows) | Baseline (`d8254003`) p50 (us) | Candidate p50 (us) | Speedup |
| --- | ---: | ---: | ---: |
| `raw_store_scan` | 2,834.0..2,974.6 (2,859.8) | **381.9..443.5 (386.3)** | **7.40x** |
| `tx_rs_scan` | 2,835.3..2,922.7 (2,874.7) | **399.3..501.2 (417.4)** | **6.89x** |
| `doc_scan` | 4,942.0..5,302.6 (4,975.7) | **2,498.8..3,199.3 (2,540.4)** | **1.96x** |
| `sql_full_select` | 5,325.8..5,536.7 (5,372.8) | **2,896.8..3,434.9 (2,963.5)** | **1.81x** |
| `json_parse_only` (reference) | 1,514.9..1,525.4 (1,519.8) | 1,547.5..1,580.7 (1,565.4) | 1.0x |
| `crc_only` (reference) | 30.5..30.9 (30.8) | 30.0..31.3 (30.8) | 1.0x |
| `seq_64k_pread` (reference) | 35.5..36.3 (35.9) | 33.5..38.3 (37.2) | 1.0x |

## Remaining gap to SQLite

- **Indexed queries (`idx_eq` 9.3 us vs. SQLite 2.7 us; `idx_range_10` 21.4 us
  vs. SQLite 2.4 us)**: remaining cost is per-query read-set transaction setup,
  metadata + index + document lookups through `serde_json::Value` decode and
  index-entry consistency re-verification on every returned row, and SQL
  execution (`src/sql/`, out of scope).
- **Full predicate scans (`full_select` ~3.0–3.2 ms vs. SQLite ~0.25–0.38 ms)**:
  log reading dropped from ~2.86 ms to ~0.39 ms (7.4x), leaving
  `serde_json::from_slice` into `serde_json::Value` (**~1.57 ms, >52%**) plus
  per-row JSON object/schema validation, key/string allocations (**~0.56 ms**),
  and SQL expression evaluation (**~0.42 ms**). Closing the remaining gap to
  SQLite's binary row format would require either a binary row representation
  or a streaming/projected JSON scan API (not possible without changing stored
  document representation or `DocumentTransaction`'s `serde_json::Value`
  return contract).

## Raw artifacts

- `sql_bench_10000.log` — raw output from 3 interleaved `sql_bench <dir> 10000 100` trials
- `sql_bench_2000.log` — raw output from 3 interleaved `sql_bench <dir> 2000 100` trials
- `checkpoint_read_probe_10000.log` — raw output from 3 interleaved `checkpoint_read_probe <dir> 10000 100` trials
- `summary.json` — parsed min/median/max tables across all trials
- `run_interleaved_bench.py` — reproducible interleaved benchmark runner
