# Early checkpoint suffix admission

Recovered read-only views and writable checkpoint stores verify the checkpoint
file against its independent digest before using its boundary for suffix-byte
admission. If the suffix exceeds the caller's cap, opening stops before reading
the covered log data or replaying the suffix. Admitted opens still verify the
covered log digest and suffix records. The separate whole-log cap is checked
against the opened log descriptor before prefix hashing and again before suffix
replay. These admission checks do not bound the total resources of an admitted
open or authenticate a log rejected for size.

The regression damages an obsolete value in the covered prefix. Both recovered
open paths report the suffix limit when the suffix is one byte over budget. At
the exact limit both detect the damaged prefix. Restoring the bytes lets both
open at that limit and return the suffix value.

## Linux ARM64 ext4 trace

The [trace archive](linux-pread-traces.tar.gz) includes the same probe source
used in both Rust 1.85.0 builds and four `strace -yy -s 0 -e trace=pread64`
captures. The baseline is `23dc66d`. The fixture puts and overwrites an 8 MiB
value before checkpointing, then appends one acknowledged put. Both APIs reject
the nonempty suffix at a zero-byte cap. Preparation is outside the traces.

| Rejected open | Baseline log bytes read | Current log bytes read |
| --- | ---: | ---: |
| Recovered read-only | 8,388,720 | 36 |
| Writable checkpoint | 8,388,720 | 36 |

The reduction equals the 8,388,684-byte covered prefix. The 36 bytes are the
log identity and header reads. Checkpoint bytes and storage-device I/O are
outside the counters. The probes used separate build directories. Their SHA-256
digests were `cbaebe5f77ffa30f74884b430cfa335a5e9abd13b46ebb9986a56e83b128d3bd`
for the baseline and `be7d46389cb7b93734a0fca0b93ca0bed6cbe8926e3c449b85a21b7afec2eab5`
for the candidate. The experiment measures logical reads and makes no elapsed
time claim.

The library and four checkpoint integration targets passed on macOS
(584 library and 26 integration tests) and Linux ARM64 (747 library and
29 integration tests). The Linux library run used 16 test threads.
