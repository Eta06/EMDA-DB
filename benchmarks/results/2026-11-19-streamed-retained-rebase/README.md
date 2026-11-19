# Streamed retained rebase

Revision `eda14da` sizes and writes the retained successor as an ordered
merge of authenticated base blocks and the existing suffix overlay. The
merge uses fixed-size I/O buffers and borrows suffix values. It does not
construct scan pages or allocate a copy of each value. The output still
needs to fit the base-file limit and its one-byte EOF probe needs to fit
the work limit. All base blocks are authenticated, including values
hidden by tombstones. The verified successor is synchronized before the
existing selection protocol runs.

## macOS rotation measurement

The [workload](rotate.rs) creates a 28 MiB managed base and appends a
deletion, a small replacement and a 4 MiB insert. Each timed process
opens a copy of the same catalog, then times `compact()` from the open
handle. Peak RSS is measured for the entire rotate process, including
open. A separate process verifies all seven live keys and all value
bytes after every rotation. The 25,166,042-byte successor files matched
after excluding the random base ID and dependent digest.

| Campaign | Build | Median peak RSS | Median compact time |
| --- | --- | ---: | ---: |
| Four pairs, concurrent test activity | Scan pages, `29137a1` | 30.34 MiB | 637 ms |
| Four pairs, concurrent test activity | Streamed, 64 KiB buffer | 6.31 MiB | 1,835 ms |
| Three repeats after library tests | Scan pages, `29137a1` | 30.34 MiB | 447 ms |
| Three repeats after library tests | Streamed, 64 KiB buffer | 6.30 MiB | 430 ms |
| Three repeats after library tests | Streamed, 1 MiB buffer experiment | 7.23 MiB | 434 ms |

The shipped variant uses the 64 KiB output buffer. Timings varied
materially between campaigns. The experiments do not isolate the
cause or establish a rotation latency improvement. The memory
measurement covers this fixture on macOS ARM64 APFS, with no claim
about allocator peaks across deployments. The old and new generations
overlap on disk. The active suffix, sparse indexes and application
allocations also consume memory outside the streaming buffers.

To reproduce, place `rotate.rs` in `examples/` at each revision and
build with `cargo build --release --locked --example rotate` using
different Cargo target directories. The first shared-target attempt
reused the baseline binary and was discarded after comparing hashes.
Run `rotate setup <new-seed-directory>`. For each fresh copy of
`<seed-directory>/managed`, run `/usr/bin/time -l rotate rotate <copy>`
and then `rotate verify <copy>`. Alternate build order between pairs.
The [environment and executable hashes](environment.txt),
[measurements](measurements.jsonl) and [phase logs](phases.tar.gz)
preserve the measured runs.

## Verification

The one-byte-short file and work ceilings leave the active selection
unchanged and no inactive slot. The exact-size rebase publishes and
reopens under the tighter limits. The fixture contains multi-chunk
base and suffix values, an empty value and tombstones. A separate test
corrupts a shadowed base value and checks that block authentication
fails and no successor is published.

At 16 threads, the libraries passed 536 tests on macOS ARM64 and 698 on
Linux ARM64. Each platform passed the 12 selected integration tests
for indexed retained reads, future log rejection and retained document
transactions. The library runs started before the shadowed-value test
was added. Both focused streaming cases passed on the final code on
both platforms, and the checked source hashes matched. Formatting and
warnings-denied Clippy passed on macOS. Physical power-loss testing and
independent off-host restore remain deferred.
