# Million-key recovery without an available checkpoint

At revision `b5d03ab`, both managed rebuild APIs recovered the same
witnessed stores on macOS ARM64 APFS (V2) and Linux ARM64 ext4 (V3).
Each fixture started with 131,072 or 1,048,576 distinct 8-byte keys and
8-byte values. Another 16,384 ordered batch children overwrote and
deleted existing keys and inserted new ones. The live count ended
4,096 above the initial count. The existing selection named a missing
checkpoint before each rebuild.

All 24 rebuilt checkpoints matched the ordinary full-log reference byte
for byte. Each fresh managed reopen scanned the entire live set against
an independent deterministic model, with a zero-byte suffix and an empty
overlay. The million-key fixture verified 1,052,672 entries on each
reopen. Source log and witness bytes matched before and after the runs.

## Resources

Three fresh-process pairs alternated rebuild order per environment and
size. Elapsed time measures rebuild and selection publication. Peak RSS
is the highest observed whole-process peak of the three runs. Reopen
and model verification ran in a separate process.

| Platform | Initial keys | Ordinary / bounded median ms | Ordinary / bounded peak RSS MiB |
| --- | ---: | ---: | ---: |
| macOS APFS | 131,072 | 91 / 753 | 35.66 / 3.09 |
| macOS APFS | 1,048,576 | 590 / 2,242 | 255.83 / 3.36 |
| Linux ext4 | 131,072 | 70 / 389 | 37.50 / 4.56 |
| Linux ext4 | 1,048,576 | 525 / 1,719 | 260.24 / 4.39 |

On Linux, each bounded rebuild, its managed reopen, and each admission
rejection ran with a **48 MiB hard process address-space limit**
(`RLIMIT_AS`). On macOS the runner checked the measured peak against
the same threshold. The ordinary comparison ran uncapped.
The bounded path used at most 2,048 distinct keys and 16 KiB of logical
key data per replay chunk, eight merge inputs, and 96 MiB of temporary
spill files. Log bytes, operation count, peak live keys, peak live-key
bytes, and checkpoint file size were set to the fixture's exact needs.
The million-key fixture had a 39,397,820-byte log and a 27,369,572-byte
checkpoint. The spill cap excludes the checkpoint, filesystem allocation
overhead, and other process allocations.

Both sizes on both platforms rejected a log cap one byte below the
fixture, a one-byte spill budget, and an output cap one byte below the
reference. All 12 rejections were classified `NotPublished(InvalidInput)`. They
left the original selection, log, and witness intact and created no
destination. These are warm-cache workloads on one host and its VM.
The measurements cover these fixture sizes and configured bounds.

## Reproduction

The [raw archive](raw-results.tar.gz) contains the exact revision,
probe, runner, platform and binary hashes, build logs, and test results.
Put `rebuild_scale.rs` under `examples/` in a scratch source export at
`b5d03ab`, then run
`cargo +1.85.0 build --locked --release --example rebuild_scale`.
Run `python3 run-scale.py /path/to/rebuild_scale /path/to/fixture-parent`
using an existing parent with at least 190 MiB free. The archive SHA-256
is `098a3480397a233704ee9a5dcfabfdb1feccd5d513324ef77c7ebfdd8745a2a3`.
