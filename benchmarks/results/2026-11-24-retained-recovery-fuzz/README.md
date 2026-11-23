# Writable retained recovery fuzzing

The fixture tests passed on macOS and Linux ARM64. They replay 14 named
regressions and 48 combinations of physical and witnessed record boundaries.
The Linux fixture tests ran on ext4.

The Linux ARM64 AddressSanitizer campaign completed 385,675 executions in
181 seconds and replayed all 140 files in the final on-disk corpus.
The campaign used nightly Rust 2026-11-03, cargo-fuzz 0.13.2, an input cap
of 128 bytes, a 10-second per-input timeout and a 512 MiB RSS limit.
The VM killed the optimized build. The campaign used a development build
with 16 codegen units and stored its temporary fixtures on tmpfs.

The model compares recovered live rows and the adopted witness against
an expected history. It also checks that rejected opens preserve the log,
base and witness. It covers standalone retained generations. Authority
transitions, concurrent writers, sync failures and physical power loss
are outside this campaign.

The [raw archive](raw-results.tar.gz) includes both Linux transcripts,
the replay corpus and matching source manifests for the host and Linux
copy. Its SHA-256 is
`9b981a857ef988b8aa34bfdee582260ae96d5e9ed2f62e90e27201b06d90f934`.
The production revision is `fe1ecd8`. The manifests identify the new fuzz
and test inputs published with this report. See the
[fuzz instructions](../../../fuzz/README.md) for reproduction.
