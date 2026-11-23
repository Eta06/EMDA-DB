# Linux kernel errors during retained archive retirement

At revision `0dca2d1`, 102 cases on a Linux ARM64 ext4 guest injected
one returned `EIO` each into the original-path cutover archive retirement
workflow. There were 51 cases for each ordinary source format, V2 and V3.
The V3 fixture verified the log marker and retained the metadata witness
xattr in its archived log.

Each fixture starts with 4,096 keys, records the original archive binding,
performs retained cutover, applies 4,096 mutations and publishes an exact
backup of the resulting authority-selected generation. The probe saves
the returned backup binding separately from the backup. Each copied
fixture preserves the original hard links and xattrs.

## Outcomes

| Injected call | Cases | Outcome and visible archive |
| --- | ---: | --- |
| Sync during authority or catalog open | 52 | Open failed. Both archive files remained. |
| Sync before removal | 28 | `NotPublished`. Both archive files remained. |
| First archive unlink | 2 | `OutcomeUnknown`. Both archive files remained. |
| Second archive unlink | 2 | `OutcomeUnknown`. The archived witness remained. |
| Sync after both unlinks | 18 | `OutcomeUnknown`. Both archive names were absent. |

The final run checked the descriptor path or unlink target for every
injection against the matching call in its successful baseline trace.
It used the same directory depth for baseline and fault fixtures. An
earlier pilot used unequal depths, which shifted ancestor-sync ordinals.
The outcomes above count only the corrected run.

Every case checked contents, modes and xattrs across the fixture.
The authority, fence, active generation, independent binding and backup
matched the pre-fault manifest. Before retry, the runner verified the
full selected state and restored and verified the independently bound
backup in a new directory. It alternated which it verified first.
A clean retry retired any remaining archive names, synchronized the
visible state and again verified the selected store.

These are one-shot errors returned by Linux under `strace`. The
campaign checks the visible filesystem state and the application's
recovery behavior. Physical power loss, persistent errors, faults on
failure-only syscall paths and independent off-host restore are outside
its scope.

## Reproduce

The [raw archive](raw-results.tar.gz) contains the two successful traces,
102 fault traces, fixture manifests, per-case results, toolchain details,
source and binary hashes, the probe patch and the runner. In a disposable
checkout, use the revision named in `source-revision.txt` and apply the
patch to add retirement-only phases to the existing lifecycle example.

```sh
git apply /path/to/evidence/retirement-probe.patch
CARGO_PROFILE_DEV_DEBUG=0 cargo +1.85.0 build --locked --example authorized_recovery_bench
python3 /path/to/evidence/archive_retirement_eio.py \
  target/debug/examples/authorized_recovery_bench /existing/ext4/scratch /new/report
```

Use a Linux filesystem supporting V3 metadata witnesses and an
environment where `strace` syscall injection is available. The runner
uses disposable copies under the scratch directory and retains traces
in the report directory.
