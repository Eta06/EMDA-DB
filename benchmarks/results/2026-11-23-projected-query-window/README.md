# Projected document query windows

Apple M6, macOS 27.0 (26A425), APFS, Rust 1.85.0 release builds. The baseline is
`8fed909f3816755055faffb613d694fa3a88eeb9`. The candidate replaces only
`src/document/filter.rs` with the version shipped with this report.
The same probe and V2 fixture were used for both builds.

The fixture contains 512 documents, each with an 8,192-element JSON array.
The query filters on a common group, ranks by an integer field, and projects one
nested integer. It skips 420 rows and returns 8. The scan uses collection order
as its input. The planned query uses the nonunique group equality index.
Each sample validates the eight ordered IDs and projections and checks that 512
rows were scanned and matched. The fixture log and witness hashes matched
before and after the measurements.

| Query path | Baseline peak RSS, MiB | Candidate peak RSS, MiB | Baseline query, ms | Candidate query, ms |
| --- | ---: | ---: | ---: | ---: |
| Scan | 112.04 (112.00–112.05) | 4.62 (4.59–4.64) | 65.72 (58.04–83.10) | 54.20 (44.76–61.80) |
| Equality index | 111.45 (111.44–111.45) | 4.08 (4.06–4.08) | 110.73 (95.01–137.86) | 82.34 (79.82–96.68) |

Values are the median and range of four runs per build and mode. Each run uses
a fresh process with previously accessed fixture data. Within each mode, pairs
run in baseline/candidate, candidate/baseline, baseline/candidate,
candidate/baseline order. RSS is the process high-water mark, including store
reopen. The query timer starts after reopen and stops before result assertions.
This workload isolates a large sorted window and a small projection on one
host. It excludes OS page cache, system-wide memory, and direct CPU and I/O
measurements. Timing can vary with local host activity.

## Reproduction

Create two scratch source copies, one from the baseline revision and one with
the candidate's `src/document/filter.rs`. Put
[the probe](projected_window_probe.rs) in each copy's `examples/` directory and
build both with `cargo +1.85.0 build --locked --release --example projected_window_probe`.
Set `baseline_bin` and `candidate_bin` to the two executable paths, and
`fixture` to a fresh directory on the measurement filesystem.

```sh
"$baseline_bin" prepare "$fixture"
shasum -a 256 "$fixture/records.log" "$fixture/commit.state" > source.sha256
for mode in scan planned; do
  for order in first second first second; do
    if [ "$order" = first ]; then
      /usr/bin/time -l "$baseline_bin" "$mode" "$fixture"
      /usr/bin/time -l "$candidate_bin" "$mode" "$fixture"
    else
      /usr/bin/time -l "$candidate_bin" "$mode" "$fixture"
      /usr/bin/time -l "$baseline_bin" "$mode" "$fixture"
    fi
  done
done
shasum -c source.sha256
```

[Raw samples](macos-samples.log) report bytes and microseconds.
