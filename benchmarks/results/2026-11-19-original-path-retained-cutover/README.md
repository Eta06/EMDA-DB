# Original-path retained cutover validation

Source revision `b64c74b`. The source hash manifest matched between the macOS checkout and the Linux ARM64 run. Both library suites used 16 test threads.

| Platform | Library tests | Selected integration tests |
| --- | ---: | ---: |
| macOS ARM64 | 528 passed | 11 passed |
| Linux ARM64 | 690 passed | 11 passed |

The integration targets were `document_retained`, `retained_indexed_candidate`, and `future_log_format_guard`. Formatting and warnings-denied Clippy passed on macOS. Platform and compiler details are in [environment.txt](environment.txt).

The cutover tests stop a child before replacing the original log and after replacement but before the parent directory sync. The first case leaves the ordinary source openable. In the second, the original path rejects the ordinary writer and the retained recovery API verifies the initial generation and synchronizes the fence before returning its registration record. The tests also exercise independent registration, append, rotation, reclamation, rejection of direct nested-catalog open and rejection after the fence is changed.

The [previous-reader probe](previous-reader-probe.tar.gz) builds revision `0ac2f8d` as a separate executable on macOS. The old executable rejected the fenced original directory through ordinary open, ordinary open with checkpoint, checkpoint store, checkpoint view and recovered checkpoint view. File paths and content hashes for the source directory, authority record, checkpoint and digest matched before and after the attempts. The current authorized executable recovered the expected key and value at the original path.

This probe covers one historical revision and five open paths. Older backups and independently writable copies require operational fencing. The authority record needs protection against rollback independent of the catalog. Interrupted preparation can leave inert directories available for inspection. The process kills exercise a running OS. Physical power-loss and independent off-host restore remain deferred.
