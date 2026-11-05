# Checkpoint lookup landmarks

The checkpoint-backed writer previously decoded entries sequentially after reading and verifying an index block. A sparse, in-memory directory now records one entry offset per 16 entries in each block. After reading and verifying the **entire same block with SHA-256**, point lookup binary-searches those offsets and scans from the nearest preceding entry. File formats, trusted checkpoint digests, log witnesses, scan behavior and the existing block-count admission rule are unchanged. The offsets are derived during verified checkpoint opening and copied with snapshot metadata. They add bounded per-block metadata, but the existing logical block and snapshot counts are not total-memory quotas.

Tests compare every present and adjacent absent key across several blocks, read-only and writable views, and ordered scans. A separate test corrupts a different entry in the same block after opening and confirms that querying the first entry still fails the full-block integrity check. Four [model and differential fuzz campaigns](../benchmarks/results/2026-11-05-checkpoint-landmark-fuzz.json) exercised 835,443 inputs without a finding; the campaigns include shared-prefix keys and both recovered and writable views. These checks are not exhaustive and do not establish physical power-loss safety.

## Paired warm-cache comparison

The [driver](../scripts/checkpoint_lookup_compare.py) creates the same witnessed fixtures for both separately built revisions, checks log, witness, checkpoint and metadata identities before and after, and alternates baseline/candidate/candidate/baseline passes. Each of ten processes per variant, mode and case opens the store and verifies 16 fixed base-key reads and up to 16 suffix reads. The table shows pooled p50 and nearest-rank p95 across those 160 repeated base-key samples; samples reuse the same keys and are not independent random requests. Key length was 128 bytes and value length 64 bytes. The 10,000- and 200,000-key fixtures each had 1,024 extra suffix keys; the 1,000-key fixture had 16, and the 16-key fixture had none.

| Filesystem / base keys | Baseline p50 / p95 (µs) | Landmarks p50 / p95 (µs) | Baseline / landmarks peak RSS (MiB) |
| --- | ---: | ---: | ---: |
| tmpfs / 16 | 2.297 / 5.694 | 2.322 / 6.044 | 1.473 / 1.479 |
| tmpfs / 1,000 | 26.313 / 53.133 | 25.177 / 53.490 | 1.535 / 1.541 |
| tmpfs / 10,000 | 33.198 / 61.130 | 31.263 / 58.878 | 1.914 / 1.922 |
| tmpfs / 200,000 | 33.625 / 60.347 | 32.956 / 60.575 | 1.980 / 2.037 |
| overlay / 16 | 2.401 / 6.979 | 2.413 / 7.018 | 1.465 / 1.471 |
| overlay / 1,000 | 26.924 / 56.248 | 25.667 / 56.611 | 1.547 / 1.547 |
| overlay / 10,000 | 33.193 / 62.110 | 31.061 / 59.977 | 1.912 / 1.906 |
| overlay / 200,000 | 33.919 / 62.213 | 33.208 / 62.421 | 1.971 / 2.014 |

The larger cases' median base-key reads improved by about 2–6% in both runs. The smallest case and some p95 samples regressed slightly; open times were mixed, and the 200,000-key candidate used about 44–58 KiB more sampled process peak RSS. The full-log store control did not use landmarks and also varied between runs. Results are specific to this workload and do not establish a universal win or competitor ranking.

CPU affinity and quiet-window checks were used on a four-CPU-quota Linux VM. One of 320 quiet checks failed; some short samples reported substantial VM steal, up to 33.3%, so tails and small differences are noisy. RSS excludes OS page cache, and the overlay backing device is unknown. These are single-client, warm-cache measurements, not cold storage, concurrent writes, total memory, physical power-loss or off-host restore tests. Raw samples, process read counters, control results, source and binary hashes, and VM/cgroup telemetry are retained for [tmpfs](../benchmarks/results/2026-11-05-checkpoint-landmark-tmpfs.json.gz) and the [workspace overlay](../benchmarks/results/2026-11-05-checkpoint-landmark-overlay.json.gz).
