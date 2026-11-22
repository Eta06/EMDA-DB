# Reusing a verified checkpoint transaction range

A checkpoint transaction in read-set mode records every live location
in the requested bounds, even when the requested page is short.
The first scan of those bounds now uses the observation to assemble
its page, including staged replacements, deletions, and insertions.
It still reads each returned value through the existing log checksum.
Later scans read the checkpoint again. The page and observation
continue to have separate limits.

## Linux ARM64 read counts

Rust 1.85 release binaries built from `c1678f1` and the candidate
used one witnessed fixture on tmpfs. The probe inserted 96 documents
and a nonunique index with four documents per group. It also inserted
two raw keys in a separate range. It verified the read results and
asserted the equality-index plan for the document query.

| Operation | Checkpoint reads before / after | Bytes before / after |
| --- | ---: | ---: |
| One one-row raw-key page | 2 / 1 | 21,284 / 10,642 |
| Same raw-key page read twice in one transaction | 3 / 2 | 31,926 / 21,284 |
| Planned nonunique document equality, four results | 9 / 7 | 95,778 / 74,494 |

These counts cover positional reads from the checkpoint descriptor
between markers after opening the store. They exclude log reads and
initial verification. They describe one fixture and do not measure
elapsed time or physical disk I/O.

## Correctness checks

The focused suites passed 67 tests on macOS and 41 on Linux ARM64.
An additional 256 chained document histories per format passed on
macOS V2 and Linux V2 and V3. Each history applies seven concurrent
calls, checks whether the observed result can serialize, and runs
both checkpoint and full-log recovery. A repeated-scan regression
checks that damage introduced after scanning still produces an error
and releases the transaction's reservations.

## Reproduction

Extract [the archive](linux-evidence.tar.gz). Copy `probe.rs` to
`examples/tx_range_probe.rs` in each revision. Build each copy in a
separate Cargo target directory. Set `baseline` and `candidate` to
the resulting binaries and `fixture` to a new directory. Run setup
once and use the same fixture for both executables.

```sh
cargo +1.85.0 build --locked --release --example tx_range_probe
"$baseline" setup "$fixture"
for operation in raw repeat document; do
  for binary in "$baseline" "$candidate"; do
    strace -qq -y -s 64 -e trace=pread64,write -o "$operation.trace" \
      "$binary" "$operation" "$fixture"
    awk -v label="$operation" -f count.awk "$operation.trace"
  done
done
```
