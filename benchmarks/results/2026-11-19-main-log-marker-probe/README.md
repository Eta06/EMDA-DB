# Historical reader check for the proposed log marker

The engine source and fixtures came from `main` at `324a082`. The
four-test integration probe came from `development` at `fdce958`. It was
copied into the exported source before compilation; the engine code was
used as committed. The Mac ran Rust 1.100.0. The ARM64 Linux VM ran Rust
1.85.0. Both ran `cargo test --locked --offline --test future_log_format_guard`
and passed all four tests.

The tested revision rejected the byte-zero `EMDALOG4` marker and truncated
prefixes through ordinary open, checkpoint and recovered-view opens. The
probe also covers an empty marked log, live backup refusal and refusal to
restore a checksum-valid backup with the marker. It compares the tested
source file contents and checks for absent backup and restore destinations.

This establishes behavior for one historical revision and the test fixtures.
A complete retained format, all supported historical entry points, and
mixed-version cutover still require verification. An independently opened
pre-cutover copy may remain readable.
