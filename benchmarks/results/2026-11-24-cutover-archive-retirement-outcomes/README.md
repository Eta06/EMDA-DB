# Cutover archive retirement failure boundaries

Source `9191132` adds a detailed retirement result while preserving the
existing publication classification. In ten Linux syscall cases, a
single returned `EIO` exercised each boundary below on both V2 and V3
source fixtures.

| Fault boundary | Detailed step | Successful log unlink | Successful witness unlink | Retry removes names |
| --- | --- | --- | --- | --- |
| Verified backup suffix sync | `Verify` | No | No | Yes |
| Archived log unlink | `UnlinkLog` | No | No | Yes |
| Archived witness unlink | `UnlinkWitness` | Yes | No | Yes |
| Catalog sync after both unlinks | `SyncCatalog` | Yes | Yes | No |
| Catalog sync when both names were already absent | `SyncCatalog` | No | No | No |

Each fault was checked against its descriptor path or unlink target.
Every case checked file contents, modes, and extended attributes
outside the expected archive removals, reopened the authority-selected
catalog, and compared all three entries in it and a store restored from
the independently bound backup. A clean retry converged on the same
synchronized state. This is returned-error evidence on a running OS.
Physical power loss and independent off-host restore remain deferred.

Native macOS Rust 1.85.0 passed 1,322 tests across 112 nonempty test
binaries. The focused cutover suite passed all 12 tests after correcting
the test fixture's comparison of macOS temporary-directory aliases.
The initial fixture failure is retained alongside the passing output.
Hosted stable and Rust 1.85.0 both passed on [macOS](https://github.com/Eta06/EMDA-DB/actions/runs/45362417452)
and [Linux](https://github.com/Eta06/EMDA-DB/actions/runs/45362417420).
Linux stable passed the optimized suite and 45 standard benchmark smoke
attempts. The four-writer recovery check passed all 51 phases, including
24 checkpoint and retained phases under a 48 MiB process address-space cap.

The [raw archive](raw-artifacts.tar.gz) preserves all of those attempts,
three additional [cross-engine smoke attempts](https://github.com/Eta06/EMDA-DB/actions/runs/45362417622),
Linux [syscall fault traces](https://github.com/Eta06/EMDA-DB/actions/runs/45362417534),
the [ENOSPC result](https://github.com/Eta06/EMDA-DB/actions/runs/45362417408),
and the local logs.
