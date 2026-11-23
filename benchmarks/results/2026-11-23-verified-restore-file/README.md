# Witness installation uses the verified staged log

The candidate changes [these source files](source.sha256) relative to
`66aeb4c`. The V3 witness is installed using the log handle used by
verification. Restore and upgrade compare the staged name and verified
length before installing the witness and again before publication.
Restore also rejects FIFO inputs while accepting explicit symlinks to
regular backups.

On macOS, the staged-log regression changes either the file identity
or its length at the end of log verification or after staged directory
sync, for both restore and upgrade. The prior revision accepted the
first altered restore. The candidate rejected all eight V2 cases,
left the destination absent, preserved the source and backup, and
allowed a complete retry. Twenty-seven backup unit tests and 25
integration tests passed on macOS with Rust 1.85.0.

On Linux ARM64 tmpfs, 25 integration tests passed with Rust 1.85.0.
The existing detailed sync probe passed 58 [restore](restore.json.gz)
and 96 [upgrade](upgrade.json.gz) one-shot EIO injections across V2 and
V3 and both reopen orders. The reports omit the host working tree
status. These cases exercise returned synchronization failures on a
running OS. Physical power-loss and independent off-host restore remain
deferred.

To reproduce, build `sync_failure_probe` with
`cargo +1.85.0 build --locked --example sync_failure_probe` and use the
existing [sync campaign](../../../scripts/sync_eio_campaign.py) with
`--publication-outcomes --operation restore` or
`--publication-outcomes --operation upgrade`.
