# Guarded index restart under write faults

The fixture starts with an unguarded unique-index build whose cursor
has already passed the first document. A read-set transaction
restarts the build with a maintenance key and writes a raw receipt.
Recovery checks that the receipt, cursor reset, and requirement for
the key appear together. It completes the recovered build inside a
rolled-back verification transaction and checks both documents and
the resulting indexes. Each outcome is checked through ordinary and
checkpoint recovery.

## V2 append boundaries

The two new library tests exercise both writers. A zero-byte append
retains the old cursor and leaves the receipt absent. A torn append
fails closed and preserves the log and witness bytes. Failure after
synchronizing the complete append recovers the guarded job and
receipt together. All six cases passed on macOS Rust 1.85.

```sh
cargo +1.85.0 test --locked --lib guarded_index_restart_recovers -- --nocapture
```

## Linux V3 witness and log failures

A public-API probe ran against the Linux ARM64 build on tmpfs and
required an `EMDALOG3` header. It captured one clean syscall trace
per writer. Fresh fixtures then injected EIO at the first DB02
append write or at the V3 witness `fsetxattr`. The traces confirm
the syscall, error and log descriptor. Both writers reported
unknown outcomes. Both recovery paths retained the old job after
the zero-byte write and recovered the complete restart after the
witness update failure. Log file hashes matched before and after
reopen in all four cases.

The [Linux archive](linux-v3.tar.gz) includes the probe, syscall
traces, injected outcomes, hash checks and related integration
tests. The probe uses the fixed synthetic key `[7; 32]` only to
check recovery of the fixture. These cases model returned errors
and torn writes on a running OS.

To repeat the V3 cases, build the archived `probe.rs` as an example
against `5614672` with Rust 1.85 on Linux. Run `probe setup FIXTURE`,
then trace `probe restart FIXTURE ordinary` or
`probe restart FIXTURE checkpoint` with
`strace -qq -y -e trace=write,fsetxattr`. Count each syscall separately
to locate the first log append after `RESTART-BEGIN` and the witness
update. For each selected ordinal, recreate the fixture and repeat
with `-e inject=SYSCALL:error=EIO:when=ORDINAL`. Reopen with
`probe check FIXTURE READER COMMITTED` using both reader names and
the expected `true` or `false`.
