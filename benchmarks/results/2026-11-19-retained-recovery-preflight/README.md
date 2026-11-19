# Early retained recovery admission

Retained open checks the physical base length against the generation's
binding and admitted byte limits before allocating a replay overlay. An
authorized reopen also compares the physical suffix length with the exact
committed and pending endpoints. The existing checksum, index, witness and
authority digest checks still govern every admitted recovery.

The standalone regression combines a short suffix with a shortened or
extended base. A bound base length mismatch rejects recovery before decoding
the suffix. The original files remain unchanged. Restoring the base and
supplying a complete tail permits normal adoption.

The authority regression starts with an acknowledged prefix and a pending
append. A proposed tail short by one byte fails length admission. A valid
record of the same size with a different value fails digest verification.
Both failures preserve the log, witness, and authority record. Recovery
adopts the exact proposed bytes when supplied and retains the acknowledged
prefix. The document writer process-kill test also checks the new early
rejection for its simulated torn tail.

With these regression tests applied to baseline `3beb2bb`, both tests
reach the suffix decoder and fail their early-admission assertions.
[Baseline authority test](baseline-authority.log),
[baseline base-length test](baseline-base.log).

The full library suites passed at 16 threads on macOS ARM64 (569 tests)
and Linux ARM64 with Rust 1.85.0 (731 tests). All 21 selected integration
tests passed on both platforms, covering retained indexed reads, retained
documents, future-format guards and atomic batches. Formatting and
warnings-denied Clippy passed on macOS. [Source hashes](source.sha256),
[macOS library](macos-library.log.gz), [macOS integration](macos-integration.log.gz),
[Linux library](linux-library.log.gz), [Linux integration](linux-integration.log.gz).
