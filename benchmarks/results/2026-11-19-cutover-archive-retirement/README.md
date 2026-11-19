# Original-path cutover archive retirement

Revision `e1eff2c` lets an authorized original-path catalog retire the
archived ordinary log and its archived witness. The operator records
the archive fingerprints independently before cutover and keeps them
for retry. The operation verifies both remaining archive names under
the cutover locks. It also requires and synchronizes a bound backup
of the exact authority-selected generation before attempting removal.

The test captures the original fingerprints before cutover, advances
the database, rotates and removes the initial retained generation.
It rejects a stale backup, substituted backup suffix, wrong log
digest and archive symlink before unlink. An injected failure after
the first unlink returns an uncertain outcome. A fresh authorized
handle resumes retirement and a further retry confirms both names
are absent. The original fence, original witness file and authority
state remain available. The ordinary opener still rejects the
fence. The backup restores all keys visible at the retirement point.

The fault injection runs on a live filesystem. It does not simulate
power loss, filesystem rollback or independent off-host restore.
The operation assumes trusted, stable directories and an independent
backup binding. Other links or backups can keep the archived inode
allocated after removal of its catalog name.

Both ARM64 hosts used the same checked source manifest. At 16 threads,
the library suite passed 541 tests on macOS and 703 tests on Linux.
Each platform passed 12 selected integration tests covering retained
document transactions, future format rejection, and indexed retained
reads. Formatting and warnings-denied Clippy passed on macOS.
