# Retained witness-write interruption

At `24197a2`, the authority-backed document test kills the writer
after the new witness bytes become visible and before witness sync.
The parent observes the complete suffix, the advanced local marker,
and the unchanged prior committed point with a pending proposal in
the independent authority. An injected witness-sync failure during
recovery returns an error and leaves all three artifacts unchanged.
A normal retry finalizes the exact proposal. The test verifies both
documents, the unique and nonunique indexes, the raw receipt, and another
acknowledged write across reopen.

The same test still covers the six earlier authority/file boundaries
and the manually truncated suffix. The standalone retained tests also
stop a writer and inject a commit failure at the added boundary. All
98 retained tests passed on macOS ARM64 and Linux ARM64 ext4.

## Extended document interruption campaign

Each platform ran the document integration suite with 25 repetitions
per delay. The ordinary and checkpoint writers were interrupted with
four schedules per repetition. Each interrupted ordinary-log image was
checked through both full-log and checkpoint recovery. Linux exercised
V3 and forced V2, and macOS exercised V2. Recoveries check acknowledged
document contents, indexes and receipts, and require the interrupted
transaction to appear as a whole or stay absent.

| Platform | Timed ordinary and checkpoint kills | Complete pending states recovered | Timed managed retained kills | Complete pending states recovered |
| --- | ---: | ---: | ---: | ---: |
| macOS APFS | 200 | 104 | 100 | 15 |
| Linux ext4 | 400 | 214 | 100 | 25 |

All 800 timed cases reopened with the acknowledged state and preserved
atomicity. The suite also covered eight deterministic before/after
authority updates per platform, and four deterministic file images
per ordinary/checkpoint writer and format combination. The invalid
truncated cases fail closed. Timed kills did not produce a torn record
in this campaign. The kernel remained running throughout. Physical
power-loss testing and independent off-host restore are deferred.

```sh
cargo test --locked --lib retained_candidate::writable -- --test-threads=16
EMDA_DOCUMENT_KILL_REPETITIONS=25 EMDA_RETAINED_DOCUMENT_KILL_REPETITIONS=25 \
  cargo test --locked --test document_kill_races -- --nocapture --test-threads=1
```

Raw results are available for [macOS retained tests](macos-retained.log.gz),
[macOS document kills](macos-document-kills.log.gz),
[Linux retained tests](linux-retained-test.log.gz), and
[Linux document kills](linux-document-kills.log.gz).
