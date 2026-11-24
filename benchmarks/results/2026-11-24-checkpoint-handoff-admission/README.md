# Checkpoint view limits during consuming compaction handoff

Source revision `d10303c992bfe05f31ff82e4404e0a626ffcd74b` applies the
current checkpoint view's file and block limits while writing the staged
replacement on Linux. The candidate publication tracker advances at the
outer directory publication attempt. A lower view limit can reject the
private candidate without changing the live path or publishing a sibling.
Independent compacted copies retain their format defaults.

The focused Linux regression uses an empty base and an acknowledged
suffix put. A file cap one byte under the required size and a zero-block
cap each return `NotPublished(InvalidInput)`. Both leave the original
log and witness bytes intact and the sibling absent. An exact byte cap
and one block admit the exchange, preserve the view limits and read the
acknowledged value from the replacement.

The [first Linux run] passed the new regression on both toolchains.
Both full-suite jobs failed at an existing integration assertion that
expected an oversized candidate to remain at the sibling path. Revision
`c525da6ab553cbe92e4ab1c08b356367338138a6` changes that test to assert
the new rejection classification and absence of a sibling, then reopens
the original store and checks the acknowledged value. It changes only
the test. The first run's logs and successful memory workload are retained.

Native macOS Rust 1.85.0 passed all 1,324 tests across 112 nonempty
binaries on the source revision. The checkpoint-view integration suite
also passed all 10 applicable tests on the follow-up revision.
Warnings-denied Clippy and rustdoc passed locally.

[First Linux run]: https://github.com/Eta06/EMDA-DB/actions/runs/45374892622

The corrected revision passed hosted [Linux] and [macOS] on stable and
Rust 1.85.0. The rejection, exact admission and managed handoff fault
regressions also passed in the optimized Linux run. The implementation
revision passed the [filesystem fault] and [ENOSPC] workflows.

The [raw archive](raw-artifacts.tar.gz) contains 98 benchmark wrapper
attempts across these revisions and the [preceding documentation push].
It includes all 90 standard smoke attempts, all five [cross-engine smoke]
attempts and all three four-writer recovery workloads. The corrected
revision's recovery workload passed 51 phases, with 24 capped at 48 MiB
of process address space. The archive includes raw logs, workload
parameters, revision identifiers and checksums.

The view caps bound the generated checkpoint file and its block count.
The preceding log copy and overall temporary disk use have separate
costs. Physical power-loss testing and independent off-host restore
remain deferred.

[Linux]: https://github.com/Eta06/EMDA-DB/actions/runs/45376266464
[macOS]: https://github.com/Eta06/EMDA-DB/actions/runs/45376266199
[filesystem fault]: https://github.com/Eta06/EMDA-DB/actions/runs/45374892860
[ENOSPC]: https://github.com/Eta06/EMDA-DB/actions/runs/45374892583
[cross-engine smoke]: https://github.com/Eta06/EMDA-DB/actions/runs/45374892534
[preceding documentation push]: https://github.com/Eta06/EMDA-DB/actions/runs/45374419959
