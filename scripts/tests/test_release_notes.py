"""Exercise release validation through its public command-line interface."""

from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / "release_notes.py"


class ReleaseNotesTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.manifest = self.directory / "Cargo.toml"
        self.changelog = self.directory / "CHANGELOG.md"
        self.output = self.directory / "notes.md"
        self.manifest.write_text('[package]\nname = "example"\nversion = "0.1.0"\n')
        self.changelog.write_text(
            "# Changelog\n\n## [Unreleased]\n\n### Added\n- Future work.\n\n"
            "## [0.1.0] - 2026-11-03\n\n### Added\n- Initial storage.\n\n"
            "## [0.0.9] - 2026-10-30\n\n### Fixed\n- Earlier change.\n"
        )

    def run_validator(self, tag):
        return subprocess.run(
            [sys.executable, str(SCRIPT), "--tag", tag, "--manifest", str(self.manifest),
             "--changelog", str(self.changelog), "--output", str(self.output)],
            capture_output=True, text=True, check=False,
        )

    def test_valid_release_outputs_only_the_matching_section(self):
        result = self.run_validator("v0.1.0")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.output.read_text(), "## [0.1.0] - 2026-11-03\n\n### Added\n- Initial storage.\n")

    def test_prerelease_tag_is_supported(self):
        self.manifest.write_text('[package]\nversion = "0.2.0-rc.1"\n')
        self.changelog.write_text("## [0.2.0-rc.1] - 2026-11-03\n\n### Changed\n- Preview behavior.\n")
        self.assertEqual(self.run_validator("v0.2.0-rc.1").returncode, 0)

    def test_malformed_tags_are_rejected(self):
        for tag in ("0.1.0", "v01.1.0", "v0.1", "v0.1.0-rc.01", "v0.1.0-rc..1", "v0.1.0+build"):
            with self.subTest(tag=tag):
                result = self.run_validator(tag)
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(self.output.exists())

    def test_package_version_must_match_the_tag(self):
        result = self.run_validator("v0.2.0")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("does not match package version", result.stderr)

    def test_release_section_must_exist(self):
        self.changelog.write_text("# Changelog\n\n## [Unreleased]\n\n### Added\n- Future.\n")
        self.assertNotEqual(self.run_validator("v0.1.0").returncode, 0)

    def test_duplicate_release_sections_are_rejected(self):
        with self.changelog.open("a") as stream:
            stream.write("\n## [0.1.0] - 2026-11-04\n\n### Added\n- Duplicate.\n")
        self.assertNotEqual(self.run_validator("v0.1.0").returncode, 0)

    def test_duplicate_undated_section_is_rejected(self):
        with self.changelog.open("a") as stream:
            stream.write("\n## [0.1.0]\n\n### Added\n- Duplicate.\n")
        self.assertNotEqual(self.run_validator("v0.1.0").returncode, 0)

    def test_release_needs_a_real_dated_entry(self):
        cases = (
            "## [0.1.0] - 2026-02-30\n\n### Added\n- Feature.\n",
            "## [0.1.0] - 2026-11-03\n\n### Added\n",
            "## [0.1.0] - 2026-11-03\n\n### Unrecognized\n- Feature.\n",
        )
        for content in cases:
            with self.subTest(content=content):
                self.changelog.write_text(content)
                self.assertNotEqual(self.run_validator("v0.1.0").returncode, 0)


if __name__ == "__main__":
    unittest.main()
