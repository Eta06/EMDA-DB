# Bounded rebuild spill entry spans

A spill cursor reads two adjacent offset slots together, then reads precisely
the entry between them. The final entry ends at the offset table. Each entry
must fill the resulting span and pass its existing location-bound checksum.
A damaged offset can invalidate both adjacent entries. The corruption
tests check that lookup and emission reject those entries, including damage
after opening the cursor. Spill layout and the whole-run digest are unchanged.

## Cross-platform validation

| Platform | Library tests at 16 threads | Targeted rebuild integration tests |
| --- | ---: | ---: |
| macOS ARM64 | 574 passed | 9 passed |
| Linux ARM64, Rust 1.85.0 | 736 passed | 10 passed |

The integration runs cover managed rebuild, a suffix beyond the recovery
overlay ceiling and a seeded full-log differential model. macOS formatting
and all-target Clippy with warnings denied passed. The initial Linux build
was killed under memory pressure. The retry passed after archiving and
cleaning an inactive VM fuzz build cache.

[Source manifest](linux-source.sha256), [macOS tests](macos-tests.log.gz),
[Linux library tests](linux-library.log.gz), [Linux rebuild tests](linux-integration.log.gz),
[initial Linux build log](linux-library-build-killed.log.gz).

## Logical spill reads

The targeted large-suffix integration test uses 16,385 eight-byte suffix
keys, a 4,096-key spill chunk and merge fan-in of four. The baseline is
revision `24367e5`. Both test binaries used Rust 1.85.0 on the same Linux
ARM64 VM, with incremental compilation and test debug symbols disabled.
The verified source trees were compiled into separate target directories.
The binary hashes distinguish the measured revisions. A first comparison
used a shared Cargo target and mistakenly reused the candidate binary for
the baseline. Those counts were discarded.

| Build | Completed spill reads | Logical bytes returned |
| --- | ---: | ---: |
| Baseline | 979,314 | 384,140,775 |
| Exact spans | 979,314 | 28,617,300 |

The exact-span build read 92.55% fewer spill bytes on this fixture. Both
test runs passed, including the fixture's full-log state comparison.
The compressed raw traces are preserved on the host in
`/tmp/emda-spill-traces.p3bVIc`.
The sums count successful `pread64` calls returning bytes for `run-NNNNNN`
files, including verification scans and excluding offset sidecar files.
They measure logical I/O for this fixture. They do not measure storage
device traffic, normal runtime, or an overall recovery limit.

```sh
strace -f -y -s 0 -e trace=pread64 -o "$trace" "$test_binary" \
  --test-threads=1 --exact \
  bounded_rebuild_restores_a_suffix_beyond_the_recovered_overlay_limit
awk '/pread64\([0-9]+<[^>]+\/run-[0-9]+>/ && / = [0-9]+$/ {
  calls++; bytes += $NF
} END {print calls, bytes}' "$trace"
```

[Read counts](linux-spill-preads.txt), [binary hashes](linux-binaries.sha256),
[trace hashes](linux-traces.sha256), [baseline result](linux-baseline-trace-test.log),
[candidate result](linux-candidate-trace-test.log).
