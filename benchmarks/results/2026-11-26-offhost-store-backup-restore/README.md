# Off-host restore of an ordinary Store backup

Revision `34b2bca8`. This drill restores one ordinary `Store` backup on an
independently identified host. It does not cover checkpoint, managed,
retained or online stores, document collection archives, other backup
modes or physical power loss.

## Procedure

`offhost_temporary_probe.rs` was a temporary example in a disposable source
copy. It is archived here and was not added to the repository.

1. On macOS (APFS), the `backup` mode wrote 100 keys of 256 bytes, deleted
   key 0, overwrote key 1 and took a `Store::backup`: 28,490 bytes. It then
   acknowledged one more write, `later`, after the backup.
2. The source directory was moved offline. Only the backup file was sent
   over SSH to AydinServer, a separate x86_64 Linux KVM guest on ext4.
3. The SHA-256 matched on both ends:
   `987b6a3912211452617fa73d41bd197e4ec11062680c904b3dd4759fba51f682`.
4. A fresh container ran the `restore` mode with networking disabled, the
   backup mounted read-only and no source mount. It ran `Store::restore`
   and reopened the store.

## Result

```text
restored_matches=99 deleted_absent=true post_backup_write_absent=true
```

All 99 live records restored, and every value was compared. The deleted
key and the write acknowledged after the backup were absent, as a backup
taken before them requires.

On the same host, `concurrent_kill_history` passed 9 tests. The logs
report 48 scenarios with `recovered=true`. This was process `SIGKILL` on
ext4, not a power failure. The host's five existing containers stayed
healthy, and the remote scratch directory was removed afterwards.

## Assumptions and limits

- Transfer: one file over SSH, checked by SHA-256 at both ends. Transfer
  corruption, a partial transfer and resumption were not exercised.
- Filesystems: APFS at the source and ext4 at the target, so the restore
  crossed operating systems and filesystems.
- Isolation: a container with no network and a read-only backup mount. The
  source directory was offline. The guest is a VM, not separate hardware.
- Failure: the drill was a clean restore. It does not test restoring after
  a host failure.
- Scale: one synthetic 28 KB store.

Physical power-loss validation remains open. It needs disposable physical
hardware with power control. AydinServer runs services and was not
rebooted.

[raw archive]: raw-artifacts.tar.gz

The [raw archive] holds the probe source, the backup file, both SHA-256
records, the restore and crash logs, the build logs and `SHA256SUMS`.
