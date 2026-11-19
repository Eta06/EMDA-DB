# Fenced retained append authority validation

Tested engine revision `95ddbea`. macOS and Linux ARM64 used matching Rust
sources, test inputs and Cargo files. See the [SHA-256 manifest](source.sha256)
and [environment record](environment.txt).

| Check | macOS | Linux ARM64 |
| --- | ---: | ---: |
| Library suite, 16 threads | 514 | 676 |
| Targeted document, indexed candidate and format guard tests | 11 | 11 |

The new tests use an in-memory compare-and-swap provider to inject an error
before or after each of the two authority updates. They check that recovery
settles on the exact prior or proposed suffix, and that an exact proposed
tail synchronizes its local witness before authority finalization.
An unregistered valid suffix and a rollback to previously witnessed bytes
both fail closed without advancing the local witness. Two byte-identical
copies racing from the same authority revision can acknowledge only one
append. The losing copy fails to reopen once the winner advances authority.

A separate [macOS previous-reader probe](previous-reader-probe.tar.gz) built
the actual preceding engine revision `0ac2f8d`, prepared a catalog with the
new roles, and attempted ordinary and managed opens with the old library.
Those opens rejected the catalog or generation. The original ordinary
source still opened, and hashes of the prepared catalog files matched
before and after the old-reader attempts. This covers the one tested
preceding revision and the separate-path format entry points.

Raw [macOS library](macos-lib.log.gz), [macOS integration](macos-integration.log.gz),
[Clippy](macos-clippy.log.gz), [Linux library](linux-lib.log.gz), and
[Linux integration](linux-integration.log.gz) logs are preserved here.
Linux used Rust 1.85.0 with offline dependencies and two build jobs.
macOS passed formatting and warnings-denied all-target Clippy.

Current evidence covers the engine's single-generation protocol and
recovery against injected provider outcomes. A deployed independent
authority, authority-selected generation rotation, and existing-path
cutover require further integration. Physical power-loss and independent
off-host restore remain deferred.
