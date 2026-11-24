# Cross-engine smoke cases after unique-tuple document lookup

The [hosted run](https://github.com/Eta06/EMDA-DB/actions/runs/45328720963)
at `1a827c3` recorded three successful smoke cases covering compound
ranges, large-value crash recovery, and concurrent document updates.
All ten files in the [raw archive](raw-artifacts.tar.gz) matched the
SHA-256 manifest after extraction.

The [timed document relationship comparison](../2026-11-24-unique-tuple-lookup-sqlite/README.md)
uses a separate verified fixture on macOS and Linux ARM64.
