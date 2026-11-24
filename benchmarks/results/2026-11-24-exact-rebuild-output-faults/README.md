# Fault validation after exact checkpoint output admission

The [raw archive](raw-artifacts.tar.gz) retains the Linux returned-error
and disposable tmpfs ENOSPC results from `bc6264f`, plus the macOS
correctness job metadata for that revision. All 126 file hashes verified
after extraction.

The rebuild and tail-open report contains 38 injected failures and four
source-log read traces across V2 and V3. The broader returned-error job
also exercised backup, checkpoint selection, and compaction. The ENOSPC
test used a disposable 8 MiB tmpfs with a 64-inode limit. Its assertions and
container cleanup passed. These are running-OS checks of the tested paths.

[Linux returned errors](https://github.com/Eta06/EMDA-DB/actions/runs/45346761814) ·
[Linux ENOSPC](https://github.com/Eta06/EMDA-DB/actions/runs/45346761709) ·
[macOS correctness](https://github.com/Eta06/EMDA-DB/actions/runs/45346761828)
