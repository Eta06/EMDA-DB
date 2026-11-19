# Buffered original-path retained cutover

Revision `31de14c` writes the retained base through a 64 KiB buffer in
private staging. It drains the buffer before syncing, rewinds the file
and validates it with bounded buffered reads. Full source verification,
the source witness recheck, publication and parent synchronization
remain in the export path. The [source manifest](source.sha256) matched
between the two test environments.

## Cutover measurements

Each platform seeded one ordinary store with 65,536 deterministic
keys, then made a fresh copy for each cutover. Four pairs of release
binaries, baseline `2d269d5` and buffered `31de14c`, ran in alternating
order. Timing includes the fresh process, source open, original-path
cutover and local authority registration.

| ARM64 filesystem and source format | Baseline median | Buffered median |
| --- | ---: | ---: |
| macOS APFS, V2 | 484.6 ms | 280.6 ms |
| Linux ext4, V3 | 183.2 ms | 95.7 ms |

Every run checked the marker at the original path, the archived log and
witness hashes, and the V3 metadata witness when present. A separate
process reopened the retained store and compared all 65,536 values.
The 7,340,176-byte bases matched within each platform after excluding
the fresh base ID and its dependent digest. The verification process
is outside the timed interval.

| Linux cutover syscalls | Baseline | Buffered |
| --- | ---: | ---: |
| `read` | 196,740 | 244 |
| `write` | 196,624 | 127 |
| `pread64` | 65,804 | 65,804 |
| `fsync` | 89 | 89 |

The [baseline trace](linux-baseline.syscalls) and
[buffered trace](linux-buffered.syscalls) each cover one additional
cutover. Their timings are excluded from the medians. The benchmark
uses local, warm copied files. The macOS samples overlapped with Linux
testing and had wider timing variation. Linux ran inside a VM. These
runs characterize the sampled cutover, with no cold-cache or
deployment-scale latency claim.

To reproduce, build `authorized_recovery_bench` with
`cargo build --release --locked --example authorized_recovery_bench`
at each revision. Run [paired_cutover.py](paired_cutover.py) with a new
output directory and paths to both binaries. Add `--trace` on Linux
with `strace` available. The [environment](environment.txt), macOS and
Linux measurements and phase logs are in this directory.

## Fault and regression coverage

The Linux kernel file-size matrix adds 12 retained cutovers covering
V2 and V3, 128-, 512- and 2,048-byte limits, and both termination on
`SIGXFSZ` and returned `EFBIG`. The staged export fits inside the 64 KiB
buffer and exceeds the kernel limit at drain. Each failed cutover
preserved the ordinary log, witness and backup. Cleanup removed owned
staging and a retry at the original path reopened both acknowledged
values. The kernel matrix passed all 48 cases across backup, restore,
upgrade and cutover. Physical power-loss validation and independent
off-host restore remain deferred.

| Platform | Library tests at 16 threads | Selected integration tests |
| --- | ---: | ---: |
| macOS ARM64 | 535 passed | 12 passed |
| Linux ARM64 | 697 passed | 12 passed |

The selected integration targets were retained indexed reads, the future
log format guard and retained document transactions. Formatting and
all-target warnings-denied Clippy passed on macOS.
