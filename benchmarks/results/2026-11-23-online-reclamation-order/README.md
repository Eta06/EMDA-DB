# Ordinary online reclamation synchronization

Ordinary online reclamation now synchronizes both sides of the retired
directory move before deleting the retired files. A scoped cleanup retry
does the same before its first unlink. Both paths synchronize the removal
of data and witness files before unlinking the store lock. This preserves the
lock name for a retry whenever retired data-file removals can still be
lost. The active replacement was selected and synchronized before
reclamation begins.

## Returned synchronization failures

The [Linux ARM64 ext4 report](linux-sync-eio.json.gz) records two successful
controls and 228 one-shot `EIO` injections. The V2 and V3 starting fixtures
each cover every observed `fsync` and `fdatasync` ordinal, with source-first
and backup-first recovery checks. The driver verifies live data and restores
a saved local backup before and after scoped cleanup. The report records
the baseline revision and modified source status used to build the probe.

| Outcome | Cases |
| --- | ---: |
| No publication attempted | 124 |
| Candidate publication uncertain | 24 |
| Exchange uncertain | 24 |
| Reclamation failed after successful handoff | 52 |
| Success despite injected error | 4 |

At each of the two post-move syncs, all eight injected failures left the
retired log, witness and lock in the owned stage. At the new sync before
lock removal, all four failures left the lock. Scoped cleanup finished
in each of these 12 cases and preserved the active state and backup.
The remaining cases cover earlier and later sync sites and can have
different staging outcomes.

Run the report verifier from the repo root.

```sh
python3 scripts/verify_reclamation_eio_results.py \
  benchmarks/results/2026-11-23-online-reclamation-order/linux-sync-eio.json.gz
```

These are returned errors on a running Linux guest. They exercise the
observable ordering and retry states. Physical power loss and independent
off-host restore were deferred.
