# Legacy upgrade failure checks

Revision `d55018cd7edaa26f7085ef100e5ceb0a7960570f` extends the Linux
filesystem-fault harness to exercise the detailed nonempty-legacy and
explicitly confirmed empty-legacy upgrade APIs. The harness records the
original legacy log, external-witness presence and metadata-witness state
before the attempt, after the attempt and after verification. It also checks
the exact rows in any visible upgraded store. The source lock file may be
created and is deliberately not treated as an unchanged source artifact.

The [retained bundle](../benchmarks/results/2026-11-14-legacy-upgrade-publication-faults.json.gz)
contains 12 one-shot campaigns, selecting observed sync, data-write, read,
open and namespace EIO calls plus sync EAGAIN calls for each upgrade API.
Each runs with a V2 and a V3 auxiliary witnessed fixture, and each injection
is repeated with both writable paths taking turns reopening that fixture.
The legacy input itself is the same unwitnessed fixture in each case; the
upgraded destination is checked through the ordinary writable path. The
auxiliary fixture, checkpoint and reference backup remain unchanged.

| Legacy input | Injections | Not published, target absent | Unknown, target visible | Unknown, target absent | Success |
| --- | ---: | ---: | ---: | ---: | ---: |
| Nonempty | 296 | 244 | 44 | 4 | 4 |
| Empty | 256 | 204 | 44 | 4 | 4 |

For nonempty legacy input, the probe maps an initial handle-open failure to
`NotPublished` before calling the detailed upgrade method. The empty-legacy
API performs its own open, so its classification includes that work.

These are synthetic returned errors at calls observed on successful paths;
they do not cover every error-only call, partial syscall, concurrent schedule
or persistence ordering. The tests ran on a Linux VM using an overlay of
unknown backing with the OS still running. They do not establish physical
power-loss safety or independent off-host recovery. A legacy log cannot
reveal data lost before the test or upgrade; the caller must independently
confirm that an empty source is intended to be empty.

The probe's six targeted tests and Clippy check passed on stable Rust and Rust
1.85; 130 Python tests passed. The parent revision's full stable, Rust 1.85 and
optimized stable suites each passed 982 tests. This change also strengthens
the independent one-shot verifier to check recorded source, checkpoint and
backup hashes for copy operations; the earlier upgrade and rollover bundles
still pass that check.

To independently recheck the retained coverage and recorded hashes:

```sh
PYTHONPATH=scripts python3 - <<'PY'
import gzip, json
from verify_one_shot_fs_results import verify
with gzip.open('benchmarks/results/2026-11-14-legacy-upgrade-publication-faults.json.gz', 'rt') as source:
    bundle = json.load(source)
print(sum(verify(entry['report'])['injections'] for entry in bundle['reports']))
PY
```
