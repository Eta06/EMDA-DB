# Disk-budget validation before managed tail adoption

Revision `fe33650e` reorders the managed checkpoint open. It used to read
the persisted disk-budget record only after adopting a complete pending
tail. A damaged record, or one combined with enabled automatic checkpoints,
was then refused as `TailAdopted`, after the witness had advanced.

Now the record is read and validated under the exclusive lock before any
durable step. Its compatibility is checked against the single stabilized
automatic configuration before automatic-stage cleanup, so compatible
stores with a disabled marker keep their cleanup. Its accounting is built
after cleanup but before tail adoption.

Three tests in `src/checkpoint_store/online/tests/budget.rs` cover:

- a damaged record with a pending tail, refused as `NotAdopted(InvalidData)`;
- a record next to enabled automatic checkpoints, refused before adoption
  with the automatic-checkpoint directory listing unchanged;
- a valid budgeted store, which still adopts its tail with the witnessed
  boundary advancing and the budget enforced.

The refusal cases keep the anchor or V2 witness, the selection, the
checkpoint and the witnessed boundary unchanged. The boundary is read
through `Log::open_checkpoint_view`, so it also covers Linux's V3
extended-attribute witness. On the previous order the damaged-record test
failed with `TailAdopted(... "disk budget record is damaged or
unsupported")` (`before-fix.log`).

The 281 checkpoint-store library tests and five checkpoint integration
suites passed locally, with warnings-denied Clippy. An independent
exact-revision native macOS audit (`native-audit/`) passed `cargo fmt
--check` and `cargo test --locked`: 125 harnesses, 1,538 tests and no
failures. On `fe33650e`, [CI], [macOS], [EIO], [ENOSPC] and [cross-engine
smoke] passed, and the three tests passed in all five test jobs.

[CI]: https://github.com/Eta06/EMDA-DB/actions/runs/45585306894
[macOS]: https://github.com/Eta06/EMDA-DB/actions/runs/45585306938
[EIO]: https://github.com/Eta06/EMDA-DB/actions/runs/45585306845
[ENOSPC]: https://github.com/Eta06/EMDA-DB/actions/runs/45585306766
[cross-engine smoke]: https://github.com/Eta06/EMDA-DB/actions/runs/45585306726
[raw archive]: raw-artifacts.tar.gz

The [raw archive] holds the before-fix and after-fix logs, the focused
logs, the native audit and the hosted run records and logs.
