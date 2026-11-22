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
