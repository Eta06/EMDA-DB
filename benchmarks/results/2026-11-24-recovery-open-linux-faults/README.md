# Linux recovery tail witness errors

[Filesystem EIO CI](https://github.com/Eta06/EMDA-DB/actions/runs/45337970569)
passed at `1cc179b5`. The [raw results](rebuild-witness-faults.jsonl)
record 24 syscall injections across ordinary open, managed rebuild, and
bounded full-log and trusted-checkpoint rebuild.

For `Store::open_with_recovery_limits_detailed`, V2 returned `NotAdopted`
when the log `fdatasync` failed. V3 returned `NotAdopted` when the final
witness read failed. Both classified witness write and sync errors as
`TailAdoptionOutcomeUnknown`. The campaign checked the visible witness,
unchanged log and checkpoint selection, and the recovered tail after a
clean retry.

These are returned Linux syscall errors. The V2 test adapter separately
covers an open error after a successful witness sync. Physical power-loss
and independent off-host restore remain deferred.
