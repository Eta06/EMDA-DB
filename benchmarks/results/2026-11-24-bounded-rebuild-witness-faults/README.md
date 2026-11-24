# Pending-tail faults across managed rebuild paths

On Linux ARM64 with Rust 1.85.0, the [campaign](../../../scripts/check_rebuild_witness_faults.py)
passed 18 syscall fault cases on three public rebuild paths. Each
case starts with the previous checkpoint selected and a complete,
unacknowledged suffix. A clean control identifies the fault points for
each path and witness format before fresh fixtures are used for EIO.

| Fault point | V2 operation | V3 operation | Expected outcome |
| --- | --- | --- | --- |
| Before the witness write | Log fdatasync | Current witness fgetxattr | NotPublished |
| Witness write | Commit-state pwrite64 | Log fsetxattr | TailAdoptionOutcomeUnknown |
| Witness synchronization | Commit-state fdatasync | Log fsync | TailAdoptionOutcomeUnknown |

All cases preserved the source log and previous selection, left the
new checkpoint absent, and verified recovery and clean retry.
The [raw results](faults.jsonl) preserve the injected syscall and the
probe response. This exercises returned syscall errors on the live
filesystem.

Reproduce on Linux with strace and xattrs from the repository root.

```sh
cargo +1.85.0 build --locked --example sync_failure_probe
python3 scripts/check_rebuild_witness_faults.py \
  --binary target/debug/examples/sync_failure_probe --directory /tmp
```
