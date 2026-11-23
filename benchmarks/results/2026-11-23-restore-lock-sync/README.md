# Restore lock synchronization and macOS interruption coverage

The candidate started from `0a7daf7`. [Source hashes](source.sha256)
identify the tested code. Restore now synchronizes the empty staged
`store.lock` before synchronizing and publishing the staging directory,
matching the lock-file ordering already used for format upgrade.

On Apple Silicon macOS, Rust 1.85.0 passed 26 backup unit tests, including
33 V2 subprocess kills and 33 injected construction/witness-stage
errors. Each construction case checks absent publication, source
preservation, staging cleanup and a successful retry. Twenty backup,
checkpoint backup, concurrent backup and upgrade integration tests also
passed.

On Linux ARM64 tmpfs, Rust 1.85.0 passed the same 20 integration tests.
Strace 7.0 ran the detailed restore sync-fault campaign at every
observed synchronization ordinal under V2 and V3, with both reopen
orders. The [one-shot](single.json.gz) and [persistent](persistent.json.gz)
reports contain 58 injected failures each. The reports omit the host
working tree status. The source hashes above identify the changed files.

Eight injections directly targeted the staged lock's `fsync` (ordinal
6 on V2 and 7 on V3). All eight returned `NotPublished`, left the
destination absent, preserved source and backup file hashes, and passed
recovery verification. Persistent sync failures left one recognized
marker per case, which cleanup removed after injection stopped.

Reproduce the Linux campaign using a disposable tmpfs or another test
filesystem and the existing sync probe. Repeat with
`--persistent-errors` and a distinct output path.

```sh
cargo +1.85.0 build --locked --example sync_failure_probe
python3 scripts/sync_eio_campaign.py --directory /path/to/disposable-fixtures \
  --binary target/debug/examples/sync_failure_probe --output /tmp/restore-eio.json \
  --operation restore --publication-outcomes
```

The process kills and returned syscall failures cover behavior on a
running OS. Physical power-loss and independent off-host recovery remain
deferred.
