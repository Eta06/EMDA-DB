# Linux syscall evidence for checkpoint recovery scan admission

The [raw archive](raw-artifacts.tar.gz) preserves the Linux filesystem
checks at `a9672c2`. All 123 file hashes verified after extraction.

Explicit and managed opens exercised both V2 and V3. A budget of 180 bytes
rejected the pending-tail fixture after 36 bytes of fixed log identity
reads, before any replay or change to the witness or selection. The exact
181-byte budget admitted it. In each of the four cases, the trace recorded
42 bytes of suffix replay, 97 bytes of covered-prefix verification and
42 bytes of confirming suffix verification. Two fixed identity reads and
the probe's final 17-byte value read are recorded outside the ceiling.

The same report preserves 38 injected failures and four other rebuild
read traces. The [hosted fault job](https://github.com/Eta06/EMDA-DB/actions/runs/45349825331)
passed. The measured reads are logical application requests on a running
OS. Filesystem block reads, caches and physical retries have separate costs.
