# Concurrent witness-write failures — 2026-11-06

On clean revision `5cecd1adaa43aae02a186a4c8de418a13519be83`, the
[local campaign](../benchmarks/results/2026-11-06-concurrent-witness-eio.json.gz)
ran four independent writers against V2/V3 stores on both writable backends.
Linux `strace` injected `EIO` into a V2 witness `pwrite64` or a V3 metadata
`fsetxattr`; injection ordinals are counted per traced thread. Each writer
recorded successful returns and stopped after its first error. Both recovery
paths took turns opening first; the verifier checked all observed successful
writes, allowed each failed write to be present or absent, and required both
paths to agree on exactly which failed writes appeared.

Twenty repetitions completed 320 cases. All 428 observed acknowledged writes
were recovered. The workers reported 1,280 failed calls; 524 of those writes
were recovered in full, demonstrating the documented unknown outcome. The
traces showed a multi-record vectored append in 80 ordinary-store cases.
Earlier acknowledged fixture values, the source log prefix, reference backup,
checkpoint and relevant witness data were also checked. A negative-control
test rejects a missing acknowledgement, and a driver test rejects recovery
paths that report different uncertain writes even when their counts match.

The [driver](../scripts/concurrent_eio_campaign.py) records the exact
receipts, injected syscalls, selected vectored appends, hashes and recovery
results. Its [GitHub workflow](../.github/workflows/sync-eio.yml) repeats a
shorter campaign. The test uses threads in one process and returned errors on
a running OS; it does not inject partial successful syscalls, exhaust all
interleavings, simulate physical power loss or perform an off-host restore.
