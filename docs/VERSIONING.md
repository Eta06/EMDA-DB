# Versioning and releases

The product name is a placeholder. The package version in `Cargo.toml` is the source of truth for the software version. The project follows [Semantic Versioning](https://semver.org/) with the policy below. Versions describe released software; Git commits by themselves do not constitute releases.

## Version numbers

- **Before 1.0:** `0.MINOR.0` may add features or introduce breaking changes; `0.MINOR.PATCH` is reserved for backward-compatible fixes. A change to the on-disk format that requires migration is breaking and must be called out explicitly.
- **From 1.0 onward:** `MAJOR` indicates incompatible public API or storage-format changes, `MINOR` adds backward-compatible functionality, and `PATCH` contains backward-compatible fixes.
- Prereleases such as `0.2.0-rc.1` are supported and published as GitHub prereleases. The release workflow does not accept build metadata (`+...`) in tags.
- Security fixes are identified in the changelog; do not disclose unfixed vulnerabilities in public release notes.

## Independent compatibility boundaries

The software version is **not** the same as the log format, public API, network protocol, or future SDK versions. Changes to those contracts must be described separately. The experimental format uses `DB01` records; new stores wrap them in an `EMDALOG2` identity header with a separate commit witness. Older unwrapped `DB01` stores remain usable with their existing limitations. `Store::upgrade_legacy` creates a verified new-format copy without replacing the source or destination. The upgrade cannot detect data already lost before conversion. Older engines cannot open new-format logs, and there is no general downgrade guarantee. Unknown or invalid markers must be rejected rather than silently interpreted as the current format. Before making a production compatibility promise, define supported upgrades and downgrades and test persistent fixtures, migrations, and backups.

## Recording changes

Add user-relevant changes to `CHANGELOG.md` under `Unreleased` when making them. Use categories as needed: `Added`, `Changed`, `Deprecated`, `Removed`, `Fixed`, `Security`, and `Limitations`. Describe behavior and migration requirements, not only internal commit names. Keep README status and compatibility information current.

## Preparing and publishing a release

The current `0.1.0` package version is a prototype and has **not** been tagged or published. When a maintainer intentionally prepares a release:

1. Choose the version using the policy above. Update `Cargo.toml` and refresh `Cargo.lock` if the package version changed.
2. Move the relevant `Unreleased` entries into a heading such as `## [0.1.0] - 2026-11-03`. Use the actual release preparation date, and leave a new empty `Unreleased` section for subsequent work.
3. Update the README, document compatibility and migration limits, and run the checks listed in `CONTRIBUTING.md`, including the release-note validator.
4. Commit and push the release preparation. Create and push a matching annotated tag, for example `v0.1.0`, only when ready to publish.
5. The release workflow validates the tag, package version, dated nonempty changelog section, and checks; it then creates a GitHub Release containing those notes. A tag with a prerelease suffix is marked as a prerelease. Existing releases are not overwritten.

The workflow currently creates release notes, not binary packages or published crates. A failed workflow does not mean a release was published; inspect its result and correct the cause. Keep published tags immutable. This workflow has not yet been exercised with a real release tag.
