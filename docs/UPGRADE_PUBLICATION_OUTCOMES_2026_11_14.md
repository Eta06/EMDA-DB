# Upgrade copy publication outcomes

Revision `cabf2a4357a1b5da827d17d7c66f78760711e67e` adds opt-in detailed
publication outcomes to format upgrade, legacy upgrade and explicitly
verified empty-legacy upgrade copies. `NotPublished` means this call did not
begin publishing the destination. A destination that already existed or
temporary staging artifacts can still remain. `OutcomeUnknown` includes a
possible publication and requires inspection before retry. The existing
methods retain their previous error types and synchronization behavior.

An upgrade of a legacy log cannot detect data lost before it was opened. The
empty-legacy variant cannot distinguish a deliberately empty log from one
emptied by loss or interruption: confirming the source's intended emptiness is
the caller's responsibility. That variant may create a source lock file even
if it does not publish the destination.

## Local checks

Integration tests exercise successful copies, missing parents, pre-existing
destinations, V2/V3 formats, a nonempty legacy fixture, an empty legacy fixture
and source preservation. A targeted injected parent-sync failure returns an
unknown outcome and the visible copy is reopened and checked.

The stable, Rust 1.85 and optimized stable all-target suites each passed 980
tests with four test threads. Both all-target Clippy checks and 129 Python
tests passed. The GitHub EIO workflow includes a small format-upgrade check.

The [retained campaign bundle](../benchmarks/results/2026-11-14-upgrade-publication-faults.json.gz)
contains six one-shot Linux campaigns against the detailed format-upgrade path
on V2/V3 fixtures, with both writable recovery paths taking turns opening
first. It injected EIO into selected observed synchronization, data-write,
read, open and namespace calls, and EAGAIN into synchronization calls. Of 316
injected cases, 264 returned `NotPublished` and left the fresh target absent;
48 returned unknown, of which 44 left a visible checked target; and four
completed successfully after an injected namespace error. Source and
reference-backup preservation were checked.

For format upgrades, the probe maps an initial handle-open failure to
`NotPublished` before calling the detailed API; the category includes both
open failures and failures returned by the upgrade method.

These selected synthetic returned errors do not exhaust failure-only calls,
partial syscalls, persistence ordering or every schedule. They exercise the
format-upgrade path; a separate [legacy-upgrade campaign](LEGACY_UPGRADE_FAILURES_2026_11_14.md)
checks nonempty and empty legacy sources. The workspace uses an overlay of
unknown backing, and the OS remained running. These results do not establish
physical power-loss safety or an independent off-host restore.

The campaign reports can be verified with:

```sh
PYTHONPATH=scripts python3 - <<'PY'
import gzip, json
from verify_one_shot_fs_results import verify
with gzip.open('benchmarks/results/2026-11-14-upgrade-publication-faults.json.gz', 'rt') as source:
    bundle = json.load(source)
print(sum(verify(entry['report'])['injections'] for entry in bundle['reports']))
PY
```
