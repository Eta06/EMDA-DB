# Versioning and releases

The product name is a placeholder. The package version in `Cargo.toml` is the source of truth for the software version. The project follows [Semantic Versioning](https://semver.org/) with the policy below. Versions describe released software; Git commits by themselves do not constitute releases.

## Version numbers

- **Before 1.0:** `0.MINOR.0` may add features or introduce breaking changes; `0.MINOR.PATCH` is reserved for backward-compatible fixes. A change to the on-disk format that requires migration is breaking and must be called out explicitly.
- **From 1.0 onward:** `MAJOR` indicates incompatible public API or storage-format changes, `MINOR` adds backward-compatible functionality, and `PATCH` contains backward-compatible fixes.
- Prereleases such as `0.2.0-rc.1` are supported and published as GitHub prereleases. The release workflow does not accept build metadata (`+...`) in tags.
- Security fixes are identified in the changelog; do not disclose unfixed vulnerabilities in public release notes.

## Independent compatibility boundaries

The software version is **not** the same as the log format, public API, network protocol, or future SDK versions. Changes to those contracts are described separately.

| Format | Records / witness | Compatibility |
| --- | --- | --- |
| Legacy | Unwrapped `DB01`; no witness | Read/write supported; complete-boundary loss can go undetected. |
| V2 | 28-byte `EMDALOG2` identity header; changing `commit.state` boundary | Read/write supported with the original two ordered syncs. |
| V3 | 28-byte `EMDALOG3` identity header; changing `user.db.commit.v3` xattr on `records.log`; immutable `commit.state` anchor at offset 28 | Current creation format on Linux with supported user xattrs; one full inode `fsync` covers data and metadata. |

Both witness encodings retain the 36-byte `EMDAWIT2` envelope: magic, 16-byte identity, little-endian 64-bit boundary, and CRC32. Single-key records use `DB01`. Atomic write batches use a checksummed `DB02` record containing ordered, independently checksummed `DB01` mutations. Batch writes require a witnessed V2 or V3 store; upgrade legacy stores first. **Older revisions that do not support `DB02` cannot reopen logs containing batches.** V2/V3 headers do not imply that old readers understand every record version. Retain a verified backup and reviewed software revision before using batches; no automatic downgrade exists. New-store creation falls back to V2 only for unsupported xattrs; an existing V3 store with missing or unsupported metadata fails closed. V3 restore requires a filesystem supporting those metadata operations. Ordinary copies of a V3 directory must preserve its xattrs, log bytes, and identity anchor; the engine backup/restore API recreates and verifies the required metadata instead.

`Store::upgrade_format(destination)` creates a verified, synchronized copy from any supported format into the current creation format at a new path. It preserves the source, log order, and existing destinations. `Store::upgrade_legacy` uses that path but accepts only unwrapped legacy sources. Upgrades cannot detect prior loss that the source format could not detect. V2 stores are never silently rewritten during opening. Older engines cannot read V3, and there is no automatic downgrade or stable format promise. Unknown or invalid markers are rejected. Persistent legacy and V2 fixtures, migration, backup/restore, corruption, and crash-state tests cover the supported paths.

Explicit index checkpoints use a separate `EMDAIDX1` file with a record-codec version, witnessed log boundary, historical live-key peak, ordered key/offset entries, log-prefix SHA-256 and file SHA-256. Future writers must increment the checkpoint record-codec version when adding a log record version older checkpoint readers cannot interpret. `checkpoint_to` returns the expected file digest; store it in trusted metadata separately from the checkpoint and provide it when reopening. Checksums are not authentication when an attacker can replace both file and expected digest. Checkpoints are unencrypted derived data, not backups, and can be discarded in favor of full-log replay. Historical peak and optional per-open limits still apply during checkpoint recovery. No stable checkpoint compatibility or automatic migration is promised before release.

Managed checkpoint selection uses a separate `EMDASEL1` file, `checkpoint.selection`, containing a bounded absolute path, the expected checkpoint digest and a SHA-256 checksum. Unknown versions and corrupt files fail closed. This checksum is not authentication: protect the selection and its store directory as trusted metadata separately from checkpoint contents. It is not included in a full-log backup; restoration creates an ordinary store and requires a fresh selection. Checkpoint paths must remain available. Older engine revisions and the ordinary `Store` do not enforce managed selection, so they can leave its suffix beyond managed recovery limits. There is no stable selection-format compatibility or automatic migration promise.

Automatic checkpointing adds an engine-owned `.emda-auto-checkpoints-v1` directory containing a versioned on/off ownership marker and two checkpoint slots. The managed selection continues to use `EMDASEL1`; the log format is unchanged. Older revisions do not enforce the automatic directory or maintain its slots. The directory and selection are trusted metadata, and there is no stable compatibility, downgrade or automatic migration promise. Full-log backups do not include these derived files.

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
