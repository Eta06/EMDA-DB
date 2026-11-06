# Staging synchronization: local restore comparison

We compared baseline `919d4cd` with `9eb5ef5`, which keeps staging ownership
markers until payload removal or publication is synchronized. The goal was to
check for a substantial regression in the successful restore path. This is an
engine before/after comparison, not a comparison with another database.

Both revisions used separately built release binaries and identical benchmark
example sources. Each restored the same verified backup five times per case,
alternating order. The timer includes validation, copying, synchronization
and publication, but excludes later hashing, reopening and sampled value
checks. Every restored-log digest matched and the sampled values passed.
Fixtures used 128-byte keys, 64-byte values and 512 suffix keys.

| Base keys | Baseline median (range), ms | Changed median (range), ms |
| ---: | ---: | ---: |
| 10,000 | 13.458 (12.132–22.266) | 13.130 (12.779–13.593) |
| 50,000 | 50.926 (48.256–78.769) | 51.624 (47.629–58.933) |

The [raw results](../benchmarks/results/2026-11-06-staging-restore-comparison.json)
include each sample, source and binary hashes, revisions and VM telemetry. The
workload ran on a Linux VM with an Intel Xeon Platinum 8573C, a four-CPU cgroup
quota and an overlay filesystem; the benchmark and driver were pinned to
different available CPUs. Both initial quiet checks passed. Individual runs
recorded up to 7.69% VM steal, with no recorded cgroup CPU throttling or direct
reclaim during the measured processes. Five warm-cache samples per revision
are too few to establish a general speed change. This test does not measure
large backlogs of abandoned stages, cold storage, competitor engines, physical
power loss or independent off-host recovery.

Reproduce with separately built binaries from the two revisions and the same
benchmark examples, then run:

```sh
python3 scripts/backup_validation_compare.py --directory /path/to/test-filesystem \
  --output /path/to/new-results.json --baseline-binary /path/to/baseline/backup_validation_bench \
  --baseline-revision 919d4cd --binary /path/to/changed/backup_validation_bench \
  --prepare-binary /path/to/changed/index_memory_bench --rounds 5 \
  --keys 10000,50000 --key-bytes 128 --tail-keys 512
```

The script names its candidate result `streaming` for historical reasons;
both compared revisions already use the streaming validator. Build and test on
the filesystem of interest and retain the complete output when comparing runs.
