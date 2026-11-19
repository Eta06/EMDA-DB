# Abandoned cutover preparation cleanup

Revision `f09ca86` takes one explicit preparation ID and verifies its
complete catalog shape and unwritten initial generation within the
recovery ceiling. Before fencing, cleanup requires the ordinary
source lock, the two original hard links, and an absent record on
the independent authority. After fencing, cleanup checks the active
fence and authority, and matches the preparation's archive against
the recorded fingerprints. The selected catalog is protected in
both cases.

Cleanup synchronizes the original-path state, moves the preparation
into a private stage, synchronizes both parents, and removes the
detached copy. A completed retry reports that the named preparation
is already absent. Private stages left by interruption use the
existing staging cleanup procedure.

The regressions exercise cleanup after a real process kill at the
pre-fence boundary. The ordinary source reopens with its acknowledged
value. Another case rejects a byte-identical archive without the
original hard link and rejects cleanup with a registered authority
while the ordinary root is visible. The fenced case retires the
active archive, preserves an unknown entry in the abandoned catalog,
then removes the verified preparation. The selected store reopens
with the expected original and later values. Existing post-fence
kill coverage also verifies recovery of the published selection.

Callers coordinate cleanup with any outstanding registration and
keep directories and the independent authority trusted and stable.
The process-kill test does not simulate physical power loss or
independent off-host restore.

macOS ARM64 passed 542 library tests and Linux ARM64 passed 704.
Both ran at 16 threads and passed 12 selected integration tests.
Linux reran all seven focused cutover tests after the final API
documentation update. The final checked source manifests match.
Formatting and warnings-denied Clippy passed on macOS.
