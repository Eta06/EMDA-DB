# Persistent format fixtures

These files preserve bytes created by earlier engine revisions. Do not rewrite a
fixture to make a newer implementation pass; add a new fixture when a supported
format changes and retain the old one for compatibility testing.

The `v3-mixed-*` fixture was created on Linux with user xattrs by revision
`066c9515c8dd1a4d752434e26a39317595340088`. It contains, in order, an ordinary
`alpha=one` put; an atomic batch that puts `beta=two` and `gamma=three`, deletes
`beta`, and puts an empty `empty` value; and a serializable transaction that
reads `alpha` and puts `delta=four`. It contains both DB01 and DB02 records.
The files are:

- `v3-mixed-records.log`: the complete log bytes, SHA-256 `201c17af1b6bece3a28a7cadb9ce956a14b4fff11e6d2c90e8d6bb28ae5f072c`.
- `v3-mixed-commit.state`: the immutable identity anchor, SHA-256 `49a869244bee4267fc213174110437a4241df18b77ce66851da0cb0e6773dde6`.
- `v3-mixed-metadata.state`: the 36-byte `user.db.commit.v3` xattr copied from the log, SHA-256 `6cf6f59b5743d2229e77568fe01ba5697af1413dbf9686bd6956e35db1332dfa`.

The Linux test restores the xattr to `records.log` before opening the store. A
normal Git checkout does not preserve file xattrs. The fixture is synthetic and
contains no production data. A successful fixture test checks compatibility
with these bytes only; it does not establish downgrade or future compatibility,
physical power-loss safety, or support on filesystems without the required xattr.

The `retained-v1-*` fixture was created on macOS APFS by revision
`23b5964d5576a35e9f7867e76778c985b7b277c1`. Its standalone
`EMDALOG4`/`EMDARW01` generation holds a base with `a=old` and
`retired=old`. The witnessed suffix first changes `a` to `new`, then
deletes `retired` and inserts a binary key and an empty value in an
atomic batch. The binding records the candidate header (112 bytes),
candidate witness (32 bytes), and base digest (32 bytes) separately
from the generation's writable witness.

- `retained-v1-base` SHA-256 `2d2209dc7bda132783b6e27030daa66d9d81b800048fc209faced2ddb1e1947e`
- `retained-v1-binding.bin` SHA-256 `bb6c8e6c3d5d002de794cf6dc3fec6100629afe9450bc9f63dfd18211bbd7c56`
- `retained-v1-commit.state` SHA-256 `cea776b72ab1304d7e4da8ff9ab9b05699637335c63f28e1b4dcb308958b4908`
- `retained-v1-records.log` SHA-256 `d33857ab551cd5ffff77483b380774701249f6c0c73d5a192b24920044c49be9`

The test makes an empty cooperating lock file, checks that the ordinary
store rejects the format, then opens the retained generation without
changing its bytes. It appends another batch and verifies the new state
after reopen. This fixture covers standalone retained generations.
