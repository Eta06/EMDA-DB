# Hosted smoke benchmarks after automatic slot fallback

All three [cross-engine smoke commands](https://github.com/Eta06/EMDA-DB/actions/runs/45308747936) on revision `44db933` passed. They exercise compound-range queries, large-value recovery, and concurrent document updates. The [archive](raw-artifacts.tar.gz) preserves the original output, attempt metadata, sample reports, workflow metadata, and the Linux stable lint diagnostics. All 12 source files matched their SHA-256 checksums after extraction.

The [Linux CI run](https://github.com/Eta06/EMDA-DB/actions/runs/45308747939) passed on Rust 1.85.0. Stable Clippy rejected the size of two variants of the restart benchmark's engine enum before any commands in that job's benchmark step ran.
