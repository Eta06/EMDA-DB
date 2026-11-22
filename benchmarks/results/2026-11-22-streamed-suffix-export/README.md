# Streaming retained suffix export

The streamed exporter copies suffix records to private staging and compares
authenticated value locations to the witnessed source. It retains source
index and touched-key metadata while using fixed-size value buffers. Export
still verifies both historical and live source state before publication.

| Export implementation | Median peak RSS | Median elapsed |
| --- | ---: | ---: |
| Baseline `16ee9af` | 175.59 MiB | 0.295 s |
| Streamed export | 2.33 MiB | 0.450 s |

The macOS ARM64 run used six fresh processes per variant on the same
fixture, alternating which variant ran first. The base held 64 keys, the
source received 128 suffix operations, and nonempty values were 1 MiB.
The suffix was 114,299,648 bytes. Every sample matched the exact suffix
digest and binding. One export from each variant also passed the harness's
full replay and live-state comparison. The timed command included source
open, verification, staging sync and publication. The comparison covers
one warm workload. Touched-key metadata still grows with distinct suffix
keys, including tombstones, and these samples establish no process cap.
[Raw samples](samples.json)

To reproduce, build the release [phase harness](../../../examples/retained_phase_bench.rs)
from each revision using separate target directories. In the fixture harness
only, pad the existing `value` result to `1 << 20` bytes with
`(i ^ seed ^ u64::from(rev)) as u8`. Set `KEYS=64 SUFFIX=128 SEED=42`
for setup, then run build and mutate once. Time each export via
`/usr/bin/time -l` and clear its output paths before the next sample.
Run `open` to verify the full live state outside the measured phase.

## Authenticate while discovering touched keys

A second paired run reused the 109 MiB suffix fixture and compared
the streamed exporter at `e2837f7` with authentication during discovery.
The latter avoids a full suffix read and hash on successful export and
offline restore. Eight fresh processes per variant alternated first
position on macOS ARM64. The harness checked each output's exact digest
and binding and performed a full replay for one output from each variant.
Elapsed time covers the same export phase as above.

| Streamed export | Median peak RSS | Median elapsed |
| --- | ---: | ---: |
| Separate digest pass | 2.31 MiB | 0.450 s |
| Digest during discovery | 2.31 MiB | 0.410 s |

The run measures export on the same warm fixture. It does not time offline
restore or characterize other workloads. [Raw samples](discovery-samples.json)

## Verify the base during prefix comparison

A third paired run used the same fixture and compared `c145a5a` with the
combined base verification and historical-prefix comparison. The frame
identifies the bound source and boundary. The comparison then authenticates
all base entries and checks the historical peaks before accepting the result.
The successful path avoids one standalone base scan.

| Streamed export | Median peak RSS | Median elapsed |
| --- | ---: | ---: |
| Separate base verification | 2.31 MiB | 0.410 s |
| Verification during prefix comparison | 2.33 MiB | 0.380 s |

Eight fresh processes per variant alternated first position on macOS ARM64.
Every export matched the known suffix digest and binding. One output from
each variant also matched the fixture's full live state after replay.
The comparison measures export on one warm workload. [Raw samples](prefix-samples.json)

## Materialize detached inspection from verified locations

The same fixture compared file inspection at `e38f2a4` with streaming
replay followed by materialization of live rows. Each of eight fresh
processes per variant verified every returned key and value. For this run,
the harness computed the last operation for each key directly to check one
row at a time. The reported inspection elapsed time ends before this model
check. Peak RSS covers the whole process, including the result check.

| File inspection | Median peak RSS | Median inspection elapsed |
| --- | ---: | ---: |
| Buffered suffix | 175.34 MiB | 0.077 s |
| Location-based replay | 58.44 MiB | 0.214 s |

Both variants used the same validated 109 MiB suffix on macOS ARM64.
First position alternated between variants. Location-based replay does
additional reads and retains the returned payload and touched-key
metadata. These samples cover a warm large-value workload.
[Raw samples](inspection-samples.json)
