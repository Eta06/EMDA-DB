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
