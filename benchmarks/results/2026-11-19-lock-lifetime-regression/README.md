# Lock lifetime validation

The offline retained restore now closes each private Store before explicitly
releasing its staging lock. Ordinary store opens retain their nonblocking
advisory lock semantics.

The Linux ARM64 VM compiled the locked library with Rust 1.85.0, no test debug
info and incremental compilation disabled. The direct test binary ran at
16 threads. The `guard-*` logs correspond to the implementation at
`fe77763`. The exact fork regression passed. Its child retained a descriptor
for the same lock inode, and an independent reopen succeeded after the original
owner was consumed and its lock released. All three full runs had 630 passes
and one failure. The checkpoint reader reopen failed once and the bounded
rebuild publication fixture failed twice, each with `WouldBlock`.

Earlier Linux logs reflect intermediate patches. The runs under `strict-N`,
`final-N` and `recheck-N` had mixed results. Some isolated launches in
those runs used a crate-prefixed test name, causing the subprocess to match zero
tests. Runs named `shared-N` are from `6c93b72` and have the same
test-selection flaw. They are retained as diagnostics, without treating the
affected test results as verification. The corrected harness requires the
child to report one passed test.

On Apple silicon macOS, the initial library run had 471 passes with 16
threads. Focused runs also passed for retained restore, detailed rebuild,
document append faults, and snapshot quota reopen. Warnings-denied Clippy
passed with the shared private-store close implementation. These tests are
running-OS evidence. Physical power loss and independent off-host restore
remain deferred.
