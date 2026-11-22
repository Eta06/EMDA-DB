# Interrupted retained authority EOF reads

The file authority checks the exact record length, reads the fixed
record, confirms EOF, and validates the record checksum under its
lock. The EOF check now uses a one-byte exact read, which retries an
interrupted syscall. A byte after the record is still an invalid
length. Other read errors propagate.

On Linux ARM64 tmpfs, two Rust 1.85 release builds ran the retained
reopen phase on the same 524,288-key fixture. Control traces found
eight successful one-byte EOF reads on `authority.record`. Each
fault run injected one `EINTR` at a selected read using the
ordinal from that binary's control trace. The archived trace confirms
the injected errno, descriptor path, and requested length for each
case. All eight baseline attempts returned `Interrupted`. All eight
fixed attempts reopened and passed the first point check. A separate
injected `EIO` still failed. The authority and retained-store file
hashes matched before and after. A subsequent full scan verified all
523,916 live entries. The focused macOS authority and retained
document suites passed 43 tests.

The [archive](linux-faults.tar.gz) includes both control traces,
ordinals, individual fault traces, record and catalog file hashes,
and the source diff against `f425517`. To repeat, use the
[recovery phase runner](../../../examples/authorized_recovery_bench.md)
to create a disposable retained fixture. Trace a clean
`authorized_recovery_bench reopen` with `strace -qq -y -s 0 -e trace=read`.
Count the `read` calls, starting at one, and select the calls which
return zero for a one-byte read on `authority.record`. Run the same
command with `-e inject=read:error=EINTR:when=N` for each selected
ordinal, starting a fresh process for each fault.

This campaign exercises returned read errors on a running OS.
Physical power-loss and off-host restore remain separate.
