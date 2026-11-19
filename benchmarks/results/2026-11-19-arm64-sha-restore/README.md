# ARM64 SHA-256 acceleration for retained recovery

A one-second profile of the location-based restore had 751 main-thread
samples. SHA-256 compression was the top frame in 629. The accelerated
build enables RustCrypto's runtime-dispatched SHA-256 backend on
little-endian ARM64 macOS and Linux. Runtime feature detection selects
the portable implementation on CPUs without SHA-256 instructions.
The native build uses the platform C compiler and assembler for
`sha2-asm`. Cargo feature resolution on x86-64 Linux continues to use
the dependency's default features. [Upstream SHA-2 backend](https://docs.rs/sha2-asm/0.6.4/sha2_asm/)

## Measured results

The restore fixture has a retained base with four 16 MiB values and a
suffix with four additional 16 MiB values. Each restore process ran the
complete new-history restore, including publication. An independent
process reopened every output and checked all eight values. The reopen
fixture has eight 16 MiB base values and an empty suffix. Its timer
covers authenticated open, and each process verified all values after
timing. All runs used a warm cache on one Apple M6. Five rounds rotated
the order of the restore builds and alternated the reopen builds. Input
file hashes matched before and after sampling.

| Restore build | Median elapsed | Median peak process RSS |
| --- | ---: | ---: |
| Materialized `03f64db` | 391.07 ms | 194.19 MiB |
| Location-based with portable SHA-256 `5e9517e` | 1,108.19 ms | 34.17 MiB |
| Location-based with ARM64 SHA-256 `70cd2c6` | 342.43 ms | 34.19 MiB |

| Eager reopen build | Median elapsed |
| --- | ---: |
| Portable SHA-256 `3574251` | 416.91 ms |
| ARM64 SHA-256 `70cd2c6` | 83.75 ms |

The accelerated location-based restore was 3.24 times as fast as the
portable location-based build at the median. Its elapsed range of
336.45–417.84 ms overlapped the materialized range of 384.76–443.16 ms.
Eager open still authenticates the entire base. These samples cover
one host and one fixture per operation. They provide process RSS for
restore and elapsed time for both operations.

## Verification

- macOS ARM64, Rust 1.100.0. All 560 library tests and 16 selected
  integration tests passed with 16 threads. Formatting and
  warnings-denied Clippy passed. The checkpoint recovery fuzz target
  compiled with the updated lockfile.
- Linux ARM64, Rust 1.85.0. All 722 library tests and the same 16
  integration tests passed at 16 threads. Build and test source file
  hashes matched the macOS checkout.
- Forced-software builds passed the retained indexed and guarded
  document integrations on both platforms. The macOS retained-base
  library tests passed with forced software and with the default
  Rust 1.85.0 build, including the independent digest fixture.
- The portable and accelerated builds restored each other's retained
  files. Each restore was opened and verified with the other build.
  Python independently checked the bound SHA-256 digests of both
  generated base and suffix files. The Linux and macOS CPU feature
  records both report SHA-256 instructions.

The Linux build completed after archiving and resetting the generated
validation target cache. The archive was checked against 3,217 file
hashes, including linked paths. The VM retained the source, test logs
and fixture directories. Physical power loss and independent off-host
restore remain deferred.

## Reproduction

Build the materialized and portable restore drivers at their listed
revisions and the accelerated driver at `70cd2c6`. Build the portable
and accelerated reopen drivers at their listed revisions. Place
`restore_memory.rs` and `base_open.rs` in the respective checkout's
`examples` directory and use `cargo build --release --locked --example`
with the example name. Copy the binaries to a scratch directory as
`materialized`, `portable`, `arm`, `reopen-portable`, and `reopen-arm`.
Run `./portable setup fixture unused` and
`./reopen-portable setup fixture-reopen`. Then run `python3 measure.py`
in that directory. The archived logs include each individual timing
and verification. The profile uses `sample <pid> 1 1` while the portable
process runs the restore command.
