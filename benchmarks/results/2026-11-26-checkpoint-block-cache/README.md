# Bounded cache of verified checkpoint blocks

A `CheckpointStore` read finds its checkpoint block through the retained
directory. Before this change it read and SHA-256-verified that block from
the file on every lookup. The store now has an optional cache of verified
blocks:

- `CheckpointStore::set_checkpoint_block_cache_limit` sets an inclusive
  byte limit. It is least recently used and shared by every read and
  snapshot of the current checkpoint.
- The default is zero, which disables the cache and keeps the old read
  path, buffer reuse included.
- A cached block is served without a file read or rehash. Logical read
  admission still charges it.
- Lowering the limit evicts down to it, and the limit cannot exceed the
  checkpoint file limit.
- The setting moves to each new checkpoint on rollover, and to the
  replacement in an online handoff. Views of the old checkpoint that
  snapshots still hold read it uncached, so only one checkpoint holds
  cached blocks. The setting resets on reopen.

`CheckpointStore::checkpoint_memory_status` reports the block directory
and landmark bytes, the overlay's keys and key bytes, and the cache's
limit, contents, hits and misses.

Verified checkpoint bytes stay correct after the file changes, because a
checkpoint is immutable once opened. Cached blocks therefore keep serving
their keys if the file is later damaged. Uncached reads still fail that
block's verification.

## Tests

`src/checkpoint_store/tests/block_cache.rs`:

- The cache is off by default. A limit above the file limit is refused.
- Repeated reads hit within the limit. Lowering the limit evicts below it,
  and zero empties the cache.
- A cached block serves correct values after every checkpoint block is
  damaged on disk. Without the cache, the same read fails with
  `InvalidData`.
- Snapshots share the current checkpoint's cache. After a rollover the new
  checkpoint starts empty with the limit, and an older snapshot still reads
  its captured state uncached. A reopen resets the limit.

Mutation checks: without eviction on a lowered limit, the bound test fails;
a cache that never serves a hit fails the damage test.

## Measurement

`examples/checkpoint_block_cache.rs` ran on macOS 27.0, Apple M6, APFS,
Rust 1.100.0, release build. A managed checkpoint held 200,000 keys with
64-byte values. Each `read` process reopened it and made 200,000 point
reads over a hot tenth of the keys, every value checked. Peak RSS is from
`/usr/bin/time -l`. There were three runs per limit, and a fresh process
does not imply a cold file cache.

| Cache limit | Reads per second | Hits | Cached bytes | Peak RSS |
| ---: | ---: | ---: | ---: | ---: |
| 0 (off) | 95,052 to 95,425 | 0 | 0 | 2.46 MB |
| 1 MiB | 109,306 to 109,870 | 34,805 | 1,048,320 | 4.15 to 5.77 MB |
| 8 MiB | 1,631,581 to 1,668,314 | 199,816 | 6,000,000 | 11.60 to 11.62 MB |

The block directory took 53,248 bytes in every run. With 8 MiB, the whole
hot set (184 blocks) fit, and throughput rose about seventeenfold. Peak RSS
grew by about 9.1 MB for 6.0 MB of cached blocks; the remainder is
allocator and map overhead the logical limit does not count. With 1 MiB,
the cache stayed within its limit while serving 17% of reads.

## Limits

The cache limit and the reported figures are logical bytes. They do not
bound total process memory: transactions, values being returned,
snapshots of earlier checkpoints and allocator overhead are outside them.

[raw archive]: raw-artifacts.tar.gz

The [raw archive] holds every run's output and time report, the summary,
the workload source, the binary hash, the host description and
`SHA256SUMS`.
