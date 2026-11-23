# Compound membership queries with gaps

Compared the compound membership planner at `bfb4fa6` with the change in
`planner.diff`, plus bundled SQLite 3.49.1 using an index on the same JSON
expressions. Each process reopened the same 768-document fixture. All
timed calls returned and checked the full ordered document contents
against the deterministic fixture. SQLite used WAL with
`synchronous=FULL`. Both planners queried the same tenant set and the
score interval `[128, 384)`.

- **Short runs** contain 136 input scores, including 120 outside the
  required bounds. The 16 survivors form two runs. EMDA changes from 32
  equality probes to four range probes and reads the same 16 candidates.
- **Wide runs** contain 136 in-bound scores in two runs. The 272-tuple
  product exceeds the 256-probe limit. EMDA changes from two broad
  tenant ranges to four exact ranges. The resulting 136 candidates are
  exactly the matches.
- **Range only** removes the membership predicate to check the path
  shared by both revisions.

Nine paired rounds alternated revision order. Within each process, 15
warmup cycles preceded 100 timed cycles per engine and case. The table
shows medians of the process medians. SQLite is from the after processes.
The fixtures were on macOS APFS and Linux ext4 in a VM on the same ARM64
host. This measures warm, single-client latency and does not establish
cold-start, concurrency, CPU or memory performance.

| Platform | Case | EMDA before µs | EMDA after µs | SQLite µs | Candidates before → after |
| --- | --- | ---: | ---: | ---: | ---: |
| macOS ARM64 | short-gapped | 32.48 | 21.60 | 48.71 | 16 → 16 |
| macOS ARM64 | wide-gapped | 285.10 | 162.19 | 77.73 | 256 → 136 |
| macOS ARM64 | range-only | 222.71 | 225.48 | 85.42 | 256 → 256 |
| Linux ARM64 | short-gapped | 31.75 | 21.12 | 46.83 | 16 → 16 |
| Linux ARM64 | wide-gapped | 293.98 | 168.23 | 76.48 | 256 → 136 |
| Linux ARM64 | range-only | 239.94 | 241.44 | 82.60 | 256 → 256 |

EMDA median latency fell about one-third for the short runs and 43% for
the wide runs on both platforms. SQLite led the wide runs and the
range-only control. These workloads are selected to measure the planner
change. They do not rank the engines across document queries.

The regression checks cover admitted exact tuples and above-cap tuple
products on both writable backends. They also cover the grouped probe
cap at 256 and the fallback beyond it. Read-set transactions conflict
with inserts in either selected interval and can commit after an
insert in the gap.

## Reproduction

Extract the [raw archive](raw-results.tar.gz). In a disposable checkout
at the revision in `revision.txt`, copy `gapped_membership_probe.rs`
to `examples/`. Build with
`cargo +1.85.0 build --locked --release --example gapped_membership_probe`.
Save that executable. Apply `planner.diff`, repeat the build, and save
the changed executable. Run
`python3 repeat-pairs.py BEFORE AFTER FIXTURE_PARENT`, redirecting stdout
to JSONL for each platform. Run `python3 summarize.py MACOS_JSONL LINUX_JSONL`
to regenerate the table. The archive contains 21,600 timings, the query
plans, environment and binary hashes. On the resource-constrained Linux
guest the first two-job release build was killed. A one-job retry built
both binaries and completed the paired campaign.
