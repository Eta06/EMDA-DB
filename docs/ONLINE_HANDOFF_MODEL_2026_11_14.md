# Online handoff reference model

Revision `8f473cbaca91dbd04301e82ec296b05f79260fe8` extends the existing
online-compaction fuzz target. Sequential inputs mix puts, deletes, atomic
batches, snapshots, reopens, reclamation and both original and detailed
successful handoffs. The model checks live and retained copies against a
`BTreeMap`. Existing destinations must remain untouched and return
`NotPublished`; invalid options must return that classification without
creating a destination.

A 90-second campaign completed **9,230 inputs** without a finding on a Linux
VM with an overlay filesystem. The [metadata and hashes](../benchmarks/results/2026-11-14-online-handoff-fuzz.json),
[raw log](../benchmarks/results/2026-11-14-online-handoff-fuzz.log.gz),
[saved corpus](../benchmarks/results/2026-11-14-online-handoff-corpus.tar.gz)
and [replay log](../benchmarks/results/2026-11-14-online-handoff-replay.log.gz)
are retained. The corpus contained 282 files; 49 force the initial store to
V2 and 233 select the default initial format, which was V3 on this filesystem.
Later compactions may create a different format. The configured maximum was
96 bytes, but the fuzzer reached a 24-byte generation limit in this short run.
Replaying all saved files completed 284 reported runs without a finding.
The two new fixed seeds also passed individually.

The model is sequential and does not inject filesystem faults, exercise
concurrent callbacks, prove all input histories or establish physical
power-loss behavior. Those limitations are separate from the selected
[returned-error campaign](ONLINE_HANDOFF_FILESYSTEM_FAILURES_2026_11_14.md).
The scheduled and manually triggered fuzz workflow already runs this target.

To replay the saved corpus on a suitable Linux machine with the pinned nightly
toolchain and `cargo-fuzz`, extract it into a new empty `extracted-corpus`
directory and run:

```sh
cargo +nightly-2026-11-03 fuzz run online_compaction ./extracted-corpus -- -runs=282 -max_len=96 -timeout=15
```
