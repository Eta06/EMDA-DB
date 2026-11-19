# Automatic rotation under independent authority

Source revision `d09fcf1`. Source hashes matched between macOS ARM64 and the Linux ARM64 guest. See [environment.txt](environment.txt) for the toolchains and filesystem details.

| Platform | Library suite at 16 threads | Selected integration and process-kill tests |
| --- | ---: | ---: |
| macOS ARM64 | 534 passed | 17 passed |
| Linux ARM64 | 696 passed | 17 passed |

Integration targets were `document_retained`, `document_maintenance_interleaving`, `retained_indexed_candidate`, and `document_kill_races`. Formatting and warnings-denied Clippy passed on macOS. The additional Linux ext4 run passed four focused library tests and one integration test.

The targeted tests exercise rotation at the configured byte threshold and at the hard suffix operation limit, rejection of a record that cannot fit an empty suffix, appends after slot reuse, and admission rechecked after maintenance. Injected failures on both sides of the authority updates for selection and reclamation preserve acknowledged data and leave the following append unsubmitted. A directory-sync fault test verifies that the retired files remain intact if synchronization of either side of the retirement move fails.

A subprocess test uses the file authority provider at a real cutover path. It stops the child before and after each of the four authority updates for rotation, reclamation, the append intent, and commit finalization. Each of the eight cases recovers the acknowledged documents, indexes and receipts. The interrupted transaction recovers as a unit. Both finalization stops recover the complete append because the local log and witness are already synchronized. The Linux run additionally repeats the focused rotation and process-kill cases with data on ext4.

This path adds synchronous work during rotation and does not establish a total process-memory or disk bound. Physical power-loss testing and independent off-host restore remain deferred.
