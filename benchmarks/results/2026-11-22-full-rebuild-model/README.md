# Full-log checkpoint rebuild model

The [existing rebuild model](../../../fuzz/fuzz_targets/bounded_rebuild.rs)
now covers the bounded full-log entry point as well as replay from a trusted
checkpoint. Generated histories include puts, tombstones, empty values and
ordered DB02 batches. The full-log cases vary key lengths from one to 1,024
bytes, leave a valid selection that names a missing checkpoint, and sometimes
restore the old witness to construct a complete unwitnessed tail.

An independent map and encoded-size model check the live state, historical
key-count peak and physical checkpoint value locations. A separate ordered
calculation determines the historical live-key byte ceiling. Exact limits
must admit the rebuild and produce the same checkpoint as ordinary full-log
recovery. Applicable one-under limits must reject before publishing or
advancing the witness. The fixed seeds exercise each of the six tested
limits, and one puts and deletes a 1,024-byte key within a single atomic
batch so peak key bytes and peak key count occur at different points.

The Linux ARM64 AddressSanitizer build completed **5,755 executions in
91 seconds** from 27 checked-in seeds. It reported no finding. The
[Linux archive](linux-arm64-model.tar.gz) contains the expanded corpus, raw
log, source hash, and compiler version. macOS ran all 27 inputs in the
[fixed-seed check](macos-seeds.log.gz), and warnings-denied Clippy passed
for the target. macOS used the V2 witness path.

```sh
run=$(mktemp -d "${TMPDIR:-/tmp}/emda-bounded-model.XXXXXX")
cp fuzz/bounded-rebuild-seeds/* "$run/"
CARGO_BUILD_JOBS=1 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 \
  cargo +nightly-2026-11-03 fuzz run --dev --codegen-units 2 \
  bounded_rebuild "$run" -- -max_total_time=90 -max_len=48 -seed=85173
```

Each generated history contains at most 12 operations. Full-log checkpoint
outputs use at most one view block. The campaign checks logical admission
and clean reopen on the running OS. It does not simulate physical power
loss, partial syscalls, concurrency, or disk and allocator exhaustion.
