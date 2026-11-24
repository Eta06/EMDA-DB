# Cumulative document transaction staging admission

Source `f19f402` adds opt-in cumulative write units and logical bytes to
the existing document read and read-set validation budgets. Replacing
or deleting a staged key still counts the current staging operation.
Document, index, metadata and raw key writes share the same budget.
Limit failures abort before append even when the callback catches
the error.

The regression runs against ordinary, checkpoint, managed retained
and authority-backed retained stores. A put, delete and second put
of the same raw key fit exactly at three units and nine bytes.
Budgets one unit or one byte below those limits reject the final put
and preserve the previously committed value. A separate case verifies read-limit
exhaustion still rolls back an admitted staged write. An indexed
insert plus a raw receipt succeeds at three write units and rejects
at two, preserving the prior document, index and receipt state.
The authority-backed case advances only for the three committed
transactions and verifies the expected state after reopening.

The native macOS focused run passed 39 tests across six document
integration binaries. Formatting, warnings-denied Clippy and
warnings-denied rustdoc also passed.

The complete native macOS Rust 1.85.0 run passed 1,324 tests across 112
nonempty binaries. Hosted stable and Rust 1.85.0 suites passed on
[macOS](https://github.com/Eta06/EMDA-DB/actions/runs/45367940470)
and [Linux](https://github.com/Eta06/EMDA-DB/actions/runs/45367940158).
Stable Linux passed the optimized suite and all 45 standard smoke
attempts. The four-writer memory gate passed all 51 phases, including
24 checkpoint and retained phases capped at 48 MiB of process address
space.

The [raw archive](raw-artifacts.tar.gz) preserves these outputs,
three additional [cross-engine smoke attempts](https://github.com/Eta06/EMDA-DB/actions/runs/45367940384),
the [filesystem fault campaign](https://github.com/Eta06/EMDA-DB/actions/runs/45367940469),
and the [ENOSPC result](https://github.com/Eta06/EMDA-DB/actions/runs/45367940343).
