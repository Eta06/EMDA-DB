# Checkpoint overlay allocations with a pinned snapshot

The probe updates one existing key in a writable checkpoint-backed store with
4,096 distinct 1,024-byte keys in the suffix. An optional read-only snapshot
pins the old overlay. Each of three fresh processes per case counts alloc,
alloc-zeroed and realloc calls and the bytes requested during the put. Setup,
snapshot capture and verification are outside the measurement. The probe
checks the active value, the older value when pinned, and the value after reopen.

| Snapshot | Baseline calls | Shared-key calls | Baseline bytes | Shared-key bytes |
| --- | ---: | ---: | ---: | ---: |
| Pinned | 4,784 | 686 | 4,636,184 | 380,872 |
| Unpinned | 6 | 4 | 2,232 | 1,152 |

All three samples in each case matched. The pinned put requested 91.8% fewer
bytes. Tree nodes still need to be copied when a snapshot pins the overlay.
These figures cover allocator requests during the tested put; they do not
measure peak RSS, CPU time, or the memory cost of constructing the overlay.

To reproduce, copy [the probe](overlay_alloc_probe.rs.txt) to
`examples/overlay_alloc_probe.rs` in a disposable checkout. Build with
`cargo +1.85.0 build --release --locked --example overlay_alloc_probe`.
Run `target/release/examples/overlay_alloc_probe pinned` and
`target/release/examples/overlay_alloc_probe unpinned`. Compare baseline
`ce79810` with the commit that adds this report. The
[raw output](results.log) records the host and compiler.
