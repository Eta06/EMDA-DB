# Bounded rebuild spill I/O faults on current code

The campaign tests revision `a6c38df7ad39d1042445033dd40a2e64756dbbd6`
on Linux ARM64 ext4, built with Rust 1.85.0 and the locked dependencies.
The [earlier campaign](../2026-11-19-bounded-spill-io-faults/README.md)
tested suffix rebuild before the current buffered spill and merge changes.

Each fixture starts with a two-key checkpoint and a nine-operation DB02
batch with overwrites, deletes, an absent-key delete, and an empty value.
One-key chunks and a merge fan-in of two exercise spill lookups and several
merge passes. Each rebuild runs with a V2 or V3 witness, either at the end
of the log or behind the complete batch. Full rebuild uses
`rebuild_managed_selection_full_bounded`. Suffix rebuild uses a trusted base
digest with `rebuild_managed_selection_bounded_detailed`.

For each of the eight fixtures, a successful `strace -yy` control identifies
the successful spill-run and offset-sidecar calls before checkpoint
publication. The runner injects one `EIO` at each identified syscall
ordinal on its own fresh fixture. It verifies that the error landed on
the expected call and run. Both paths exercise `openat`, `pread64`,
`read`, `write`, `lseek`, `statx`, and `unlinkat`.

| Rebuild | Cases per witness format and position | Total cases | Clean retries matching ordinary replay |
| --- | ---: | ---: | ---: |
| Full log | 709 | 2,836 | 2,836 |
| Trusted-base suffix | 565 | 2,260 | 2,260 |
| Total | | 5,096 | 5,096 |

Every failed attempt must return `NotPublished` with the injected errno,
preserve the log, base checkpoint, and prior selection, leave the destination
absent, and remove its owned staging artifacts. The V3 identity anchor
must remain byte-identical. The witness may stay at the original boundary
or advance to the exact validated tail boundary, depending on whether
the error occurs before or after tail adoption. Each clean retry must
select a checkpoint with no overlay, return the expected keys and values,
and match the ordinary full-replay checkpoint byte for byte. All 5,096
injected failures left the witness at its original boundary. The clean
retries on unacknowledged fixtures advanced the witness to the end of the
complete batch.

All four macOS ARM64 V2 controls also match ordinary replay, covering both
rebuild paths and witness positions under Rust 1.85.0.

## Reproduction

The [archive](raw-results.tar.gz) contains the Rust probe, Python runner, build and environment
logs, eight successful control traces, and the checked injection rows
with their `strace` lines and retry verdicts. Extract it, copy
`spill_io_probe.rs` into `examples/` in a checkout of the tested revision,
and build with `cargo +1.85.0 build --locked --example spill_io_probe`.
Use an absolute path to the resulting executable and an ext4 fixture
workspace with V3 xattr support. Choose a fresh result directory for
each command.

```sh
python3 spill_io_campaign.py PROBE WORKSPACE NEW_SUFFIX_REPORT --mode suffix
python3 spill_io_campaign.py PROBE WORKSPACE NEW_FULL_REPORT --mode full
```

This campaign covers returned `EIO` for one pre-publication spill call
on each recorded fixture, followed by an error-free retry. Multiple
faults in one run, interruptions against this revision, and errors
after checkpoint publication are outside this campaign. Physical
power-loss testing and independent off-host restore remain deferred.
