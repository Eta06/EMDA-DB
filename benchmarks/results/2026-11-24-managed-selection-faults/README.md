# Managed checkpoint selection fault boundaries

Linux ARM64, Rust 1.85.0, tmpfs, revision `5ba9273`. V2 and V3
fixtures both started with a managed checkpoint and acknowledged data.
Manual rollover and offline full-log rebuild each ran through the
existing one-shot EIO harness. Both recovery backends took turns
reopening first after every fault.

| Operation | Namespace injections | Synchronization injections |
| --- | ---: | ---: |
| Manual managed rollover | 24 | 112 |
| Offline managed rebuild | 28 | 108 |

All 272 injections and their recovery checks completed. The eight
successful controls and all per-case results are in the
[raw evidence](raw-results.tar.gz). Some namespace faults hit cleanup
and the main operation succeeded. Failures syncing the selection's
staged marker or payload left the old selection visible and returned
the checkpoint classification. EIO from the selection rename or a
subsequent ancestor sync returned the selection classification. Manual
rollover blocked access after the selection-classified failures.

To reproduce, build `sync_failure_probe` and run
`scripts/sync_eio_campaign.py --publication-outcomes` with either
`checkpoint-managed-rollover-existing` or
`checkpoint-managed-rebuild-existing`. Run both `--syscall-set namespace`
and `--syscall-set sync`, then check each JSON with
`scripts/verify_one_shot_fs_results.py`. Use a disposable fixture directory
and new output paths. These tests inject returned Linux syscall failures
on the running OS.
