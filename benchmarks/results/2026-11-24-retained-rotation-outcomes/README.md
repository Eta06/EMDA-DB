# Retained rotation publication outcomes

A Linux ARM64 running-OS fault test injected one `EIO` at a time into
`fsync` calls of a managed retained rotation on tmpfs. The baseline
[trace](baseline.log) identifies the publication renames and sync order.
Each fault started from a copy of the same fixture. The [fault traces and
reopen results](faults.log) record the returned error variant and injected
ordinal. Each reopen checked both previously acknowledged values.

| Injected call | Outcome | Visible generation on reopen |
|---|---|---|
| Successor stage marker sync | `NotPublished` | Previous |
| Catalog sync after successor rename | `SuccessorOutcomeUnknown` | Previous |
| Private selection file sync | `SuccessorOutcomeUnknown` | Previous |
| Catalog sync after selection rename | `SelectionOutcomeUnknown` | Successor |

Before selection, the test compared the active metadata byte for byte and
checked whether the inactive slot existed. After the selection rename it
required the successor to open with the original values. A failed sync in
this test does not establish crash persistence or physical power-loss behavior.
The authority update cases were exercised separately by the macOS unit suite
with injected provider failures before and after replacing the authority.

To reproduce in a disposable Linux checkout, copy the
[probe source](retained_rotation_fault_probe.rs.txt) to
`examples/retained_rotation_fault_probe.rs` and build it with
`cargo +stable build --offline --locked --example retained_rotation_fault_probe`.
Run the probe with `prepare <existing-empty-directory>`, then copy that
fixture before running `rotate <fixture-copy>`. Trace with
`strace -qq -y -e trace=fsync,rename,renameat,renameat2`. Use the clean trace
to find each ordinal and repeat on new copies with
`-e inject=fsync:error=EIO:when=N`. Run `reopen <fixture-copy> old` or
`reopen <fixture-copy> new` to check the corresponding generation.
