# Chained document transaction histories at the supported test limit

Release builds at `b5d03ab` checked 4,096 windows per witness format on
macOS ARM64 APFS and Linux ARM64 ext4. Each window ran seven concurrent
calls and carried its actual final state into the next window. The model
checked the recorded responses and final state against a real-time serial
ordering. It covered document and indexed reads, unique-index claims
and releases, and cancellation.

The ordinary and checkpoint stores alternated as writers. Both reopened
after each window and checked the entire modeled state. The ordinary
store wrote a new checkpoint every 16 windows.

| Environment | Witness | Windows | Attempted calls | Recovery checks | Checkpoint refreshes |
| --- | --- | ---: | ---: | ---: | ---: |
| macOS APFS | V2 | 4,096 | 28,672 | 8,192 | 256 |
| Linux ext4 | V2 | 4,096 | 28,672 | 8,192 | 256 |
| Linux ext4 | V3 | 4,096 | 28,672 | 8,192 | 256 |
| Total | | 12,288 | 86,016 | 24,576 | 768 |

These runs sample concurrent schedules with clean closes and reopens.
Injected storage failures and interrupted processes are covered by
separate campaigns.

```sh
EMDA_CHAINED_DOCUMENT_MODEL_WINDOWS=4096 cargo +1.85.0 test --release --locked \
  --test document_transaction_model chained_document_histories -- --nocapture
```

Use an ext4 temporary directory with user xattr support to reproduce the
Linux V3 run. The [raw logs](raw-results.tar.gz) retain source hashes and
actual outcome counts. The archive SHA-256 is
`a44d5b369211c8a6788b381f08e791e2a2a05cb01c03c1256b544f79e9a7478f`.
