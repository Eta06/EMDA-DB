# Authority-selected rotation validation

Tested engine revision `4b4648e`. macOS and Linux ARM64 used matching Rust
sources, test inputs and Cargo files. See the [SHA-256 manifest](source.sha256)
and [environment record](environment.txt).

| Check | macOS | Linux ARM64 |
| --- | ---: | ---: |
| Library suite, 16 threads | 518 | 680 |
| Targeted document, indexed candidate and format guard tests | 11 | 11 |

Rotation tests verify successor selection after authority responses are
lost before or after CAS. The two slots remain intact on uncertainty.
Recovery follows the exact selected header and suffix even when the local
selector is removed. It fails closed when the selected base is missing or
corrupt, preserving the prior witness. An unconfirmed reclamation fence
leaves the inactive generation intact. Successful cases reclaim and reuse
both slots with acknowledged values retained.

## Local process-kill probe

The [probe archive](linux-process-probe.tar.gz) includes the Rust harness,
build log, mount evidence and outputs. A Linux ARM64 VM kept each test catalog
on tmpfs and the independent test record on ext4. The harness paused a child
immediately before the selection compare-and-swap or immediately after its
record replacement and parent sync. The parent killed the child at the
reported boundary and verified that both slots were still present. Recovery
selected the prior generation in the first case and the successor in the
second case. Each recovered the acknowledged value, reclaimed the inactive
slot and acknowledged a new value.

The probe adapter assumes the parent runs one updating process at a time.
It only tests process interruption and the engine's use of a separately
persisted record. A deployed provider needs durable, concurrent CAS and
independent rollback protection. Physical power-loss and independent
off-host restore remain deferred.

To repeat the probe, set its Cargo dependency to a checkout of `4b4648e`
and build offline in release mode. For each boundary create a fresh catalog
and authority path with `authority-rotation-probe seed CATALOG RECORD`.
Launch `authority-rotation-probe rotate CATALOG RECORD before` or use
`after` as the last argument. After the child prints its boundary, send
SIGKILL to that child and wait for it to exit. Run
`authority-rotation-probe recover-before CATALOG RECORD` or
`recover-after` to check the corresponding outcome.

Raw [macOS library](macos-lib.log.gz), [macOS integration](macos-integration.log.gz),
[Clippy](macos-clippy.log.gz), [Linux library](linux-lib.log.gz), and
[Linux integration](linux-integration.log.gz) logs are preserved here.
Linux used Rust 1.85.0, cached offline dependencies and two build jobs.
macOS passed formatting and warnings-denied all-target Clippy.
