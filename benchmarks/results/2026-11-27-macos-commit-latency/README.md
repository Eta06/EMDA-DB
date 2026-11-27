# macOS APFS durable commit latency: V2 barrier-ordered sync

Interleaved before/after measurements on one macOS APFS host comparing baseline
commit `63adbb78` (two `F_FULLFSYNC` calls per V2 commit) against the V2
barrier-ordered sync implementation (`F_BARRIERFSYNC` on `records.log` followed
by mandatory strict `F_FULLFSYNC` on `commit.state`, with no on-disk format
change), alongside SQLite 3.49.1 configured with matching durability
(`journal_mode=WAL`, `synchronous=FULL`, `fullfsync=ON`, and
`checkpoint_fullfsync=ON` where applicable).

## Host and environment

- OS: macOS 27.0 (Build `26A425`, `Darwin 27.0.0`, `RELEASE_ARM64_T8152`)
- CPU / RAM: Apple M6 (12 cores), 16 GiB RAM
- Storage: internal SSD, APFS (local mount reporting `MNT_LOCAL` and
  `VOL_CAP_INT_BARRIERFSYNC`)
- Toolchain: `rustc 1.100.0 (0ef1bc386 2026-11-09)`,
  `cargo 1.100.0 (3acc79c0c 2026-11-05)`, release profile (`--release --locked`)
- Concurrency caveat: other developer builds/tests could run on this host
  during the measurement window, and APFS drive-flush latency exhibits ~1 ms
  quantum steps and transient I/O contention; trials were strictly interleaved
  with alternating execution order to control for host drift.

## Profiling baseline and design rationale (reused from `6d7f3162` / exploration commit `39a4a67a`)

The raw APFS syscall floor and per-commit phase split below were measured on
this same host against baseline commit `6d7f3162` (preserved on exploration
branch `t3/macos-commit-latency` at `39a4a67a`) and are reused here as profiling
context:

### Raw APFS syscall floor (reused from `6d7f3162`, 120 interleaved rounds)

| Operation | Avg | p50 | p90 | p99 |
| --- | ---: | ---: | ---: | ---: |
| 1-file write + `F_FULLFSYNC` (floor) | 3,021.0 us | 2,994.0 us | 3,957.0 us | 4,017.0 us |
| 1-file write + `F_BARRIERFSYNC` | 358.1 us | 364.0 us | 418.0 us | 535.0 us |
| 1-file write + plain `fsync` (non-durable ref) | 33.7 us | 30.0 us | — | — |
| V2 baseline pattern (2x `F_FULLFSYNC`) | 5,266.9 us | 5,538.0 us | 5,808.0 us | 7,171.0 us |
| V2 barrier pattern (`F_BARRIERFSYNC` + `F_FULLFSYNC`) | 3,501.5 us | 3,902.0 us | 3,979.0 us | 4,134.0 us |
| V3 xattr pattern (`fsetxattr` + 1x `F_FULLFSYNC`) | 3,433.6 us | 3,140.0 us | 4,015.0 us | 4,043.0 us |

### Per-commit phase split in release `Store` (reused from `6d7f3162`, n = 100)

- `Store::put` (V2 baseline): total avg **6,118.1 us** (p50 **6,005.8 us**, p99 **8,081.8 us**)
  - `log_sync` (`F_FULLFSYNC` #1): **3,231.3 us (52.8%)**
  - `witness_sync` (`F_FULLFSYNC` #2): **2,825.7 us (46.2%)**
  - All other work (`log_len`, `log_write`, `witness_read`, `witness_write`,
    encoding, coordinator, index publication): **61.1 us (1.0%)**
- `Store::transaction`: avg **6,068.1 us** (`log_sync` 3,163.8 us + `witness_sync` 2,835.6 us)
- `DocumentDatabase::insert`: avg **6,148.3 us** (`log_sync` 3,306.3 us + `witness_sync` 2,768.9 us)

### Why only V2 barrier-ordered sync was shipped (V3-on-macOS deferred)

The V2 barrier-ordered path (`F_BARRIERFSYNC` on `records.log` followed by
strict `F_FULLFSYNC` on `commit.state`) rests directly on Apple's documented
`fcntl(2)` two-phase ordering and drive-cache flush semantics, preserves the
existing `EMDALOG2` format for both new and existing macOS stores, and delivers
virtually the same latency as V3 (`3,501.5 us` vs. `3,433.6 us` raw average).
Enabling V3 (`EMDALOG3`) on macOS was considered and deferred because Apple does
not explicitly document power-loss persistence of APFS extended attributes under
`F_FULLFSYNC`, and doing so would change the on-disk format of every new macOS
store.

## Change measured (`63adbb78` vs. V2 barrier-ordered sync)

- On local macOS APFS and HFS+ volumes (`fstatfs` `MNT_LOCAL`) reporting
  `VOL_CAP_INT_BARRIERFSYNC` via `fgetattrlist`, when `records.log` and
  `commit.state` share the same device (`st_dev`), witnessed V2 stores sync
  `records.log` with `fcntl(F_BARRIERFSYNC)` (retrying `EINTR`, falling back
  strictly to `F_FULLFSYNC` on `ENOTSUP`/`EOPNOTSUPP`/`EINVAL`/`ENOTTY`, and
  propagating all other errors such as `EIO` without touching `commit.state`),
  followed by mandatory strict `fcntl(F_FULLFSYNC)` on `commit.state` (retrying
  `EINTR` only, never silently degrading to plain `fsync(2)`).
- Unwitnessed legacy logs, cross-device V2 stores, and non-APFS/HFS+ or network
  volumes retain full syncs.
- macOS store creation (`EMDALOG2`) and V3-on-macOS rejection (`Unsupported`)
  are unchanged from `63adbb78`.

## Interleaved before/after results (`63adbb78` baseline vs. candidate)

All benchmarks ran 3 interleaved trials with alternating order (`baseline,
candidate` in trials 1 and 3; `candidate, baseline` in trial 2). Ranges are
`min..max` across the 3 trials with median in parentheses.

### 1a. `sql_bench <dir> 10000 100` (10,000-row bulk load in 100-row batches + 100 single-row durable ops)

Note: at `63adbb78`, `examples/sql_bench.rs` completes and reopens/verifies the
four primary backends (`emda-store`, `emda-checkpoint`, `documents`, `sqlite`)
and then fails in the subsequently added `emda-store-indexed` phase with
`Error: Bounds("snapshot key count limit exceeded")` (10,000 rows in `items1` +
10,000 rows with 3 secondary indexes in `items3` exceed the snapshot key count
cap). Both baseline (`63adbb78`) and candidate exhibited this identical
pre-existing error; `examples/sql_bench.rs` was not modified. Section 1b below
also measures `sql_bench <dir> 2000 100`, where all 7 backends complete cleanly
with exit code 0.

| Build | Backend | Load (ms) | Insert p50 (us) | Insert p99 (us) | PK Update p50 (us) | PK Delete / Delete p50 (us) |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Baseline (`63adbb78`) | `emda-store` | 443.0..716.3 (605.1) | 4,192.3..6,028.0 (5,003.8) | 7,086.9..8,150.8 (8,134.0) | 4,096.7..6,950.7 (5,873.4) | 4,062.9..5,991.8 (5,033.8) |
| Candidate | `emda-store` | **389.2..477.1 (409.0)** | **3,894.1..3,952.7 (3,925.0)** | **4,011.9..6,891.4 (5,994.1)** | **3,961.0..3,979.1 (3,962.3)** | **3,934.5..3,972.9 (3,970.3)** |
| Baseline (`63adbb78`) | `emda-checkpoint` | 578.4..656.3 (629.3) | 3,124.0..5,961.2 (5,945.1) | 7,961.8..8,980.5 (8,111.6) | 2,979.6..5,976.5 (5,854.6) | 3,152.7..5,987.5 (5,302.0) |
| Candidate | `emda-checkpoint` | **373.9..377.1 (376.1)** | **1,397.0..3,921.8 (3,076.4)** | **4,250.5..6,506.3 (4,683.5)** | **1,994.7..3,953.9 (3,925.3)** | **1,428.2..3,964.0 (3,800.4)** |
| Baseline (`63adbb78`) | `documents` | — | 3,499.2..5,055.0 (5,020.0) | 5,384.2..7,953.1 (7,030.8) | — | 3,931.3..5,108.7 (4,993.1) |
| Candidate | `documents` | — | **1,184.0..3,966.4 (3,954.1)** | **3,524.2..6,892.7 (4,196.3)** | — | **1,150.5..3,972.2 (3,958.0)** |
| Reference (baseline runs) | `sqlite` (WAL + `fullfsync=ON`) | 165.6..357.0 (187.8) | 1,362.8..3,171.8 (2,126.4) | 4,057.4..4,936.1 (4,109.3) | 1,227.8..3,083.5 (1,682.6) | 1,147.1..3,093.9 (1,996.9) |
| Reference (candidate runs) | `sqlite` (WAL + `fullfsync=ON`) | 192.1..353.9 (342.0) | 1,126.5..3,092.3 (3,036.2) | 4,018.2..5,943.4 (4,082.0) | 1,132.8..3,058.0 (3,024.0) | 1,081.8..3,137.0 (3,044.0) |

### 1b. Supplementary `sql_bench <dir> 2000 100` (all 7 backends including secondary-indexed SQL tables, exit code 0)

| Build | Backend | Load (ms) | Insert / `insert_1idx` p50 (us) | Insert / `insert_1idx` p99 (us) | PK Update / `insert_3idx` p50 (us) | PK Delete / `insert_3idx` p99 (us) |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Baseline (`63adbb78`) | `emda-store` | 61.8..140.1 (137.2) | 3,029.1..5,995.8 (5,951.1) | 7,659.9..15,494.6 (9,126.4) | 3,288.4..6,051.2 (5,983.1) | 3,034.2..6,067.8 (5,978.8) |
| Candidate | `emda-store` | **79.1..318.3 (103.2)** | **3,073.8..7,002.4 (3,906.4)** | **4,183.0..15,771.8 (5,028.0)** | **3,486.0..7,063.5 (3,942.4)** | **3,097.8..7,029.0 (3,891.2)** |
| Baseline (`63adbb78`) | `emda-checkpoint` | 87.1..195.9 (119.0) | 3,769.6..5,991.8 (5,970.1) | 7,114.8..12,893.8 (7,129.6) | 3,304.8..6,023.2 (5,985.5) | 3,413.0..6,041.0 (5,999.4) |
| Candidate | `emda-checkpoint` | **79.1..164.0 (102.0)** | **3,098.3..7,121.2 (3,909.5)** | 8,522.9..13,765.2 (8,923.1) | **3,003.7..7,072.4 (3,854.2)** | **2,831.5..7,046.3 (3,960.2)** |
| Baseline (`63adbb78`) | `documents` | — | 3,462.1..5,993.0 (5,940.0) | 6,971.8..7,607.0 (7,249.2) | — | 3,412.0..6,002.9 (5,972.4) |
| Candidate | `documents` | — | **1,248.8..4,949.7 (3,919.2)** | **4,035.5..22,941.5 (6,931.5)** | — | **1,432.9..4,070.0 (3,935.8)** |
| Reference (candidate runs) | `sqlite` (WAL + `fullfsync=ON`) | 35.6..71.0 (61.2) | 1,104.1..3,923.3 (2,994.7) | 2,994.2..4,103.4 (4,040.1) | 1,053.5..3,935.3 (3,016.1) | 1,082.7..3,952.2 (3,007.6) |
| Baseline (`63adbb78`) | `emda-store-indexed` | — | 4,087.4..5,959.8 (5,881.2) | 7,098.3..22,368.3 (12,949.6) | 4,041.1..6,028.5 (6,002.8) | 6,998.2..8,817.5 (7,996.9) |
| Candidate | `emda-store-indexed` | — | **1,070.6..3,865.3 (3,834.0)** | **3,633.6..14,939.2 (5,965.4)** | **1,167.6..3,986.6 (3,983.4)** | **3,869.6..4,322.9 (4,178.2)** |
| Baseline (`63adbb78`) | `emda-checkpoint-indexed` | — | 3,832.4..6,807.8 (5,925.5) | 7,846.0..15,884.0 (9,184.2) | 4,001.3..6,010.9 (6,009.3) | 7,880.8..14,178.9 (9,991.5) |
| Candidate | `emda-checkpoint-indexed` | — | **3,800.2..3,953.8 (3,831.6)** | **3,985.7..19,186.1 (4,338.1)** | **3,938.8..3,994.2 (3,984.7)** | **4,310.5..10,359.5 (4,327.7)** |
| Reference (candidate runs) | `sqlite-indexed` (WAL + `fullfsync=ON`) | — | 3,036.8..3,937.0 (3,919.7) | 6,030.4..10,391.2 (7,540.1) | 3,920.6..3,996.8 (3,945.9) | 5,335.8..16,482.9 (6,197.2) |

(For the `*-indexed` rows above, the last four numeric columns are `insert_1idx` p50, `insert_1idx` p99, `insert_3idx` p50, and `insert_3idx` p99.)

### 2. `transaction_bench` (`disjoint`, 1 client x 200 ops and 8 clients x 100 ops)

All runs completed with 0 conflicts and verified final counters after reopen.
SQLite runs enforce `journal_mode=WAL`, `synchronous=FULL`, `fullfsync=ON`, and
`checkpoint_fullfsync=ON`.

| Clients | Build / Mode | Throughput (tx/s) | p50 (us) | p99 (us) |
| ---: | --- | ---: | ---: | ---: |
| 1 | Baseline `readset` | 145.7..165.7 (163.1) | 5,997.8..6,043.7 (6,001.0) | 6,985.9..13,939.0 (8,969.2) |
| 1 | Candidate `readset` | **260.1..284.1 (268.0)** | **3,041.2..3,993.7 (3,959.4)** | **4,145.5..6,022.5 (5,004.0)** |
| 1 | Baseline `checkpoint-readset` | 162.4..164.6 (163.4) | 5,997.3..6,001.7 (6,000.8) | 7,083.2..11,070.0 (9,087.7) |
| 1 | Candidate `checkpoint-readset` | **252.9..272.1 (267.4)** | **3,927.5..3,998.2 (3,958.4)** | **4,060.5..7,186.8 (5,571.0)** |
| 1 | SQLite (`fullfsync=ON`) | 280.6..304.6 (293.0) | 3,016.8..3,926.8 (3,018.7) | 4,121.7..8,221.7 (5,027.0) |
| 8 | Baseline `readset` | 649.4..662.8 (656.3) | 11,999.3..12,004.9 (12,004.2) | 13,152.6..18,211.7 (16,170.5) |
| 8 | Candidate `readset` | **790.7..997.5 (993.7)** | **7,999.0..8,005.2 (8,000.2)** | **8,274.7..13,866.3 (11,001.8)** |
| 8 | Baseline `checkpoint-readset` | 156.6..164.6 (159.1) | 48,010.1..49,025.2 (49,023.3) | 55,062.8..67,025.1 (58,930.8) |
| 8 | Candidate `checkpoint-readset` | **240.8..250.7 (242.2)** | **31,998.9..32,011.4 (32,008.5)** | **37,014.4..46,021.2 (41,025.6)** |
| 8 | SQLite (`fullfsync=ON`) | 233.5..250.2 (234.5) | 3,194.4..3,992.2 (3,894.3) | 18,161.9..18,276.0 (18,230.3) |

### 3. `kv_bench` (`prototype`, 64 keys, 64B value, 100% writes)

`kv_bench`'s `sqlite` mode does not enable `PRAGMA fullfsync=ON`, so it is
omitted from this durability-matched comparison; SQLite comparisons use
`sql_bench` and `transaction_bench` above. All trials reported
`verified_reopen=true`.

| Clients | Ops/client | Build | Throughput (ops/s) | Write p50 (us) | Write p99 (us) |
| ---: | ---: | --- | ---: | ---: | ---: |
| 1 | 200 | Baseline (`63adbb78`) | 143.0..168.5 (144.8) | 6,006.0..6,800.1 (6,051.7) | 8,039.1..22,382.9 (13,874.1) |
| 1 | 200 | Candidate | **200.4..254.2 (222.7)** | **3,984.9..4,067.1 (4,019.8)** | **6,025.8..17,005.6 (6,957.3)** |
| 8 | 100 | Baseline (`63adbb78`) | 557.6..596.2 (577.7) | 13,975.3..15,007.8 (14,203.8) | 22,954.1..25,208.2 (23,910.0) |
| 8 | 100 | Candidate | **969.8..1,047.3 (1,019.1)** | **7,963.0..8,046.2 (7,975.2)** | **10,089.9..25,974.8 (10,544.7)** |

## Findings and limits

- Single-writer durable commit latency on macOS APFS drops from ~6.0 ms median
  p50 to ~3.9–4.0 ms median p50 across `transaction_bench`, `kv_bench`, and
  `sql_bench` (~34% lower p50, +54–64% single-writer throughput), closing nearly
  the entire gap to SQLite's ~3.0–3.9 ms p50 (`fullfsync=ON`) while retaining
  the `EMDALOG2` format and external witness.
- Concurrent disjoint writers (`transaction_bench` 8 clients, `kv_bench` 8
  clients) gain +51–76% median throughput (`993.7 tx/s` vs. SQLite's `234.5
  tx/s` in `transaction_bench` `readset`, and `1,019.1 ops/s` in `kv_bench`) and
  cut p50 latency by ~33–44%.
- Indexed SQL inserts (`sql_bench 2000 100`, 1 and 3 secondary indexes) drop
  from ~5.9–6.0 ms p50 to ~3.8–4.0 ms p50, matching `sqlite-indexed` (~3.9 ms
  p50).
- **Limits**: measurements come from one Apple M6 internal APFS SSD with
  potential concurrent host activity; physical power-cut testing was not
  performed on macOS (correctness relies on Apple's documented `F_BARRIERFSYNC`
  and `F_FULLFSYNC` semantics in `fcntl(2)`). Non-APFS/HFS+ volumes, network
  mounts, cross-device V2 stores, and unwitnessed legacy logs keep full syncs.

[raw archive]: raw-artifacts.tar.gz

The [raw archive] contains all fresh `63adbb78` vs. candidate trial logs
(`sql_bench_10000.log`, `sql_bench_2000.log`, `transaction_bench.log`,
`kv_bench.log`), parsed `summary.json`, benchmark driver
(`run_interleaved_bench.py`), the reused `6d7f3162` syscall probe sources
(`reused_6d7f3162_sync_probe2.c`, `reused_6d7f3162_vol_caps.c`), and
`SHA256SUMS`.
