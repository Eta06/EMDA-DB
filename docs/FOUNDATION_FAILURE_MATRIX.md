# Foundation failure and recovery coverage

This inventory maps the current contract to concrete local evidence. It is not
a statement of production readiness or proof of hardware behavior. The
[storage and transaction contract](STORAGE_TRANSACTION_CONTRACT.md) defines the
actual guarantees and assumptions; the [foundation follow-up](FOUNDATION_FOLLOW_UP.md)
tracks work that remains open. "Process kill" means termination while the OS
continues running and does not model loss of kernel or device caches.

| Area | Current evidence | Important remaining limits |
| --- | --- | --- |
| Store initialization and identity | [Ten selected creation boundaries](INITIALIZATION_AND_STAGING_BOUNDARIES_2026_11_06.md) are interrupted with returned errors and process kills. V2/V3 truncated headers and damaged V3 identity anchors fail closed and preserve evidence. | Tests do not inject every kernel failure within every write or directory operation, nor establish persistence across real power loss. |
| Atomic commits | [Synthetic persistence-state matrix](../tests/commit_crash_matrix.rs) covers log prefixes crossed with torn V2/V3 witnesses on both writable recovery paths. [Actual atomic commits](../src/store/commit_process_tests.rs) are killed at selected append, synchronization and witness boundaries for both writers. Successful recovery keeps the earlier acknowledged values and exposes the whole batch or the prior state; partial records fail closed. | The synthetic model covers specified prefixes, not every storage reorder or checksum collision. Process termination is not power failure, and a failed request can have an unknown outcome. |
| Independent group commits | [Actual grouped-write kills](../src/store/group_commit_process_tests.rs) cover selected append, sync and witness boundaries, with a prior acknowledged write, three queued writers and both recovery paths. A [concurrent-writer kill race](../tests/concurrent_kill_history.rs) checks acknowledged unique writes from four workers on both writer and recovery paths. Other fault tests cover every byte of a partial vectored append and group synchronization errors. A [kernel file-size-limit test](../tests/kernel_file_limit.rs) exercises both SIGXFSZ termination and returned EFBIG after zero or partial single-record appends on both writer and recovery paths; it preserves earlier bytes and witnesses. | Independent writes are not one atomic batch. A partial tail can make the store unavailable until investigated; file-size limits are not a physical ENOSPC or power-loss simulation, and the tests do not exhaust concurrent schedules or storage failures. |
| Backup, restore and upgrade | [Interruption tests](INITIALIZATION_AND_STAGING_BOUNDARIES_2026_11_06.md) cover staged writes, witness installation, publication and retry for V2/V3. Backup tests also exercise truncation, corruption, existing destinations and injected ancestor-sync failures. | These are local copies on a running OS. An independent off-host restore and physical power-loss drill remain deferred; checksums are not authentication or a substitute for independent backups. |
| Directory creation and publication | [Directory tests](../src/directory.rs) inject errors at each resolved ancestor; backup/restore/upgrade and checkpoint tests check failures after publication. The caller receives an uncertain result when publication may have happened. | Symlink alias directories are caller-owned. Not every possible filesystem error, partial directory update or noncooperating process is simulated. |
| Staging and cleanup | [Cleanup ordering tests](../src/staging/ordering_tests.rs) check parent synchronization, marker retention on injected failure, live-lock exclusion, hard-link fallback and process kills. The staging cleanup fuzzer exercises recognized and malformed entries, symlinks and retry. | Cleanup assumes trusted, cooperating directories and is not secure erasure. OS-level tests cannot prove physical persistence ordering. |
| Checkpoints and compaction | The [foundation follow-up](FOUNDATION_FOLLOW_UP.md) links targeted fault, interruption and reference-model tests for checkpoint selection, rollover and compaction. Verified copies are checked before publication, with uncertain outcomes after some sync failures. | Coverage is selected rather than exhaustive; online reclamation, bounded memory and bounded recovery remain incomplete. |
| Transactions and concurrency | Short serial-history models, deadline/cancellation races and document/index recovery checks are linked from the [foundation follow-up](FOUNDATION_FOLLOW_UP.md). | The models are finite and do not cover every interleaving, multi-process writer configuration or deployment workload. |

Completed checks establish behavior only under their named models and the
contract's filesystem, synchronization and cooperative-lock assumptions. The
prioritized local work remains broader fault and concurrent-history coverage,
representative memory/I/O and repeated-restart measurements, and a later
comparable cross-engine rerun. The separately deferred physical power-loss and
independent off-host restore drills must remain open until actually performed.
