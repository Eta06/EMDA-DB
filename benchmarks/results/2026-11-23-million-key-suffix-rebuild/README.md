# Million-key rebuild from a trusted checkpoint and pending suffix

The standalone probe used revision `74d302d` with Rust 1.85.0 and locked
dependencies on macOS ARM64 APFS and Linux ARM64 ext4. The ext4 fixture
ran in a local VM.

The probe created a checkpoint for 1,048,576 keys, then appended 16,384
ordered operations in atomic batches. The suffix includes deletes,
replacements and inserts. After generating a reference checkpoint using
ordinary full-log recovery, the runner restored the original witness.
The resulting fixture has a complete unacknowledged suffix and the
original checkpoint still selected.

Each platform exercised three rejection cases with a too-small log,
spill or output limit. Each returned `NotPublished(InvalidInput)`, left
the source log, witness, trusted checkpoint and prior selection unchanged,
and created no destination. The admitted run checked the exact log,
base and output file lengths and a historical live-key ceiling of
1,052,672. It used chunks of at most 2,048 keys and a merge fan-in of eight.

| Filesystem | Witness | Rebuild elapsed ms | Rebuild peak RSS MiB | Reopen and scan peak RSS MiB |
| --- | --- | ---: | ---: | ---: |
| macOS APFS | V2 | 746 | 3.22 | 2.70 |
| Linux ext4 | V3 | 463 | 2.77 | 2.65 |

Each rebuild produced a checkpoint byte-identical to ordinary replay.
The new witness matched the previously acknowledged endpoint, and the
source log and trusted checkpoint kept their original hashes. A separate
managed reopen compared every ordered key and value with an independent
formula for the workload. It verified 1,052,672 entries and an empty overlay.

Linux ran each rejection, rebuild and reopen under a 48 MiB process
address-space cap. macOS measured resident memory for the same phases.
The figures describe one run per platform, with fixture creation outside
the measured phases. The configured spill limits exclude checkpoint
output and filesystem allocation overhead.

## Reproduction

The [raw archive](raw-results.tar.gz) contains the exact revision, Rust probe,
Python runner, source hashes, and unedited transcripts.
Its SHA-256 is `b3df8c3244bf3640907247255ad8d4043b4250fdd0a7f5600c3a174fabcbfa81`.
Extract it and place `suffix_rebuild_scale.rs` under `examples/` in a
scratch export of that revision. Build and run with an existing fixture
parent that has at least 155 MiB free.

```sh
cargo +1.85.0 build --locked --release --example suffix_rebuild_scale
python3 run.py "$(pwd)/target/release/examples/suffix_rebuild_scale" /path/to/fixtures
```
