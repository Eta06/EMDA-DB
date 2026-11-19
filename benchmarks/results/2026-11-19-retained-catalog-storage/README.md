# Retained catalog storage admission

Revision `9f44c50` adds `open_with_storage_limits` to both managed
retained catalog types. The optional policy measures logical regular
file bytes and directory entries under the catalog lock. At an
authorized original path the measurement includes the entire source
root, with archives, abandoned preparations and staging artifacts.
Opening checks the bound before tail adoption. Append charges the
encoded record before the authority update or append. Compaction
reserves successor and selection staging, then sizes the base to fit
within the remaining allowance. Reclamation accounts for its own
marker and directory entries.

The five focused cases cover the following behavior.

- An exact append reaches the cap. A subsequent write is rejected
  with the log and witness intact. An unaccepted tail beyond the
  cap cannot advance the witness during recovery.
- A retained private stage counts until cleanup. Cleanup frees
  space for the same append.
- A managed successor publishes at the reserved byte and entry
  ceilings. One byte or one entry below the reservation leaves
  the prior selection intact and no inactive slot.
- Authorized automatic rotation charges reclamation headroom.
  At one entry below the required reservation it cannot update
  authority. At the exact reservation the triggering append and
  reclamation complete and both values reopen.
- An original-path archive consumes budget. Rejected appends leave
  authority unchanged. Retirement frees room for a write, and a
  symbolic link in the measured root is rejected.

This is a per-open logical budget. The same policy must be installed on
subsequent opens. Admission walks directory metadata within the live
lock, so work grows with the admitted entry count. Hard links are
charged per path. Filesystem allocation, initial publication, and
authority and backup storage outside that root are governed separately.
Authority providers for this mode must keep their files outside the
measured tree. Physical power-loss testing and independent off-host
restore remain deferred.

macOS ARM64 passed 547 library tests and Linux ARM64 passed 709.
Both used 16 threads and passed 12 selected integration tests.
The Linux kernel file-size suite also passed its 54 exercised cases.
The checked source manifests matched between hosts. Formatting and
warnings-denied Clippy passed on macOS.
