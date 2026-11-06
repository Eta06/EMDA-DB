# Concurrent transactions and process kills

The [transaction kill test](../tests/concurrent_kill_history/transactions.rs)
holds one transaction inside its callback after it has staged a counter update
and a unique marker. Four other workers run transactions that atomically
increment the same counter and write their own unique markers. Workers retry
reported conflicts and send a receipt after each successful commit. The parent
kills the process after observing the held transaction and a selected number of
receipts, then drains complete events from the pipe.

When recovery succeeds, both writable recovery paths must agree. Every
observed receipt must match its marker; the counter and all recovered markers
must form a contiguous sequence with no duplicates or per-worker gaps; and
the deliberately unsubmitted transaction's marker must be absent. The test
also verifies the earlier guard value and lock exclusion. If an incomplete or
invalid tail is rejected, it instead checks that the log and witness remain
unchanged and that the other recovery path also rejects it. This is the
prototype's fail-closed behavior, not a claim that every damaged tail can be
automatically repaired.

## Recorded run

On clean revision `fdbff7535791c4fd8e67b5f92157856084c78b4d`, 100 release
repetitions covered V2/V3, ordinary/checkpoint writers and receipt targets of
1, 8, 32 and 128. The [raw result](../benchmarks/results/2026-11-06-concurrent-transaction-kill.json.gz)
records the 1,600 cases, command, binary hash and environment.

| Writer and format | Cases | Observed acknowledgements | Recovered commits |
| --- | ---: | ---: | ---: |
| Ordinary V2 | 400 | 17,276 | 17,669 |
| Checkpoint V2 | 400 | 17,329 | 17,722 |
| Ordinary V3 | 400 | 17,588 | 17,983 |
| Checkpoint V3 | 400 | 17,810 | 18,203 |

All 1,600 cases reopened through both recovery paths, preserving all 70,003
observed acknowledgements and excluding the held transaction's marker. The
71,577 recovered commits include transactions whose receipts the parent did
not observe before termination; an unobserved result is not a confirmed abort.
No case in this run took the fail-closed branch. These deliberately contended
workloads are not throughput or conflict-rate benchmarks.

The test used SIGKILL on a shared Linux VM's workspace overlay. The OS and
device caches remained powered on, and the schedules sampled are finite. It
does not establish physical power-loss behavior, off-host restoration, all
transaction interleavings or a guarantee that every non-held worker was in
commit at the instant of termination. A single run can be reproduced with
`cargo test --release --locked --test concurrent_kill_history transactions::`.
