# Linux interrupted-call campaign

The [raw archive](raw-artifacts.tar.gz) preserves all nine reports at
`0177f26`, including the progress files, hosted job metadata and coverage
verifier output. All 20 embedded file hashes verified after extraction.

The campaign ran 234 controls and **4,906 one-shot `EINTR` injections**
across synchronization, reads and writes, extended attributes, metadata,
paths, locks, opens and namespace operations. All nine reports passed
independent coverage verification. The tested operation returned success
in 2,880 cases and an error in 2,026. The recovery oracle checked both
outcomes against the documented guarantees.

The campaign targets syscalls observed on successful fixture paths and
simulates their return values while the OS keeps running. Failure-only
paths, multiple simultaneous errors, physical power loss and independent
off-host restore have separate coverage requirements.

[Hosted run](https://github.com/Eta06/EMDA-DB/actions/runs/45345732074) ·
[Failure matrix](../../../docs/FOUNDATION_FAILURE_MATRIX.md)
