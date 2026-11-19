# Indexed retained base and detached suffix validation

The tested sources are at `8bc7f1d`. The source hashes matched on both
hosts. Linux ARM64 with Rust 1.85.0 passed the 633-test library suite at 16
threads. Apple silicon macOS with Rust 1.100.0 passed all 51 retained
library tests, all-target Clippy with warnings denied, and formatting.
Both hosts passed all five indexed-reader integration tests. The Mac's
471-test full library run used the base index commit `1f4d9a7` before
the overlay was added.

The integration tests check point reads and paginated results under exact
limits, malformed and replaced inputs, rereading changed blocks, and
merged byte ceilings. They include two 16 MiB values in the same logical
scan, empty values, overwrites, batch deletes and a tombstone for a key
missing in the base. The library test compares pages against ordinary
replay across eight generated histories, 16 bound pairs and three page
limits per history.

## Paired local read measurement

Both paths read the same verified synthetic dataset. It has 16,385 base
keys, 16,385 suffix operations, and 14,044 final keys. The release probe
consumed the entire result and hashed each ordered key/value pair. All ten
processes reported 14,044 keys and checksum `1c6afbee4f80ff7a`.

| Path | Median wall time | Median peak RSS |
| --- | ---: | ---: |
| Materialized replay, then consume | 0.03 s | 5,275,648 bytes |
| Indexed open, then scan in 1,024-entry pages | 0.06 s | 4,571,136 bytes |

Five processes per path ran in alternating order on the Apple silicon
Mac. Compilation was outside timing. The first materialized run was
0.24 s and the other four were 0.03 s. OS caches were not cleared.
This small full-scan workload measures process peak RSS and elapsed time.
It does not measure point-query latency, large-base scaling, allocator
overhead separately, or a total recovery-memory ceiling.

To reproduce, extract `synthetic-input.tar.gz` into a new fixture directory.
Copy `probe.rs` to `src/main.rs` in a temporary Cargo package named
`retained-index-probe`, version `0.1.0`, edition 2024. Set its sole
dependency to the local `database-engine` checkout at the tested commit.
Use `probe.Cargo.lock` as the package lockfile. Build with
`cargo build --release --offline --locked` and run
`/usr/bin/time -l target/release/retained-index-probe MODE FIXTURE`
for `materialized` and `indexed`. The recorded samples are in
`macos-probe.log.gz`.

The handle verifies a detached read-only snapshot. Durable writable
recovery, generation selection and live reclamation remain open.
